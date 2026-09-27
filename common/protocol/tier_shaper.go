package protocol

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// 按池整形：一个池一对上下行整形器，节点上所有限速都由它执行。
//
// 池由控制面显式指定（MemoryUser.Pool）。同池的用户对象——不管挂在几个入站、几种协议上——
// 共用同一份速率与突发额度；Pool 为空时以 email 为池，再没有 email 就按对象各自成池。
//
// 速率只有两种来源，谁决定取决于节点调度器（node_fairshare.go）有没有判定拥塞：
//
//	不拥挤   标准封顶；有突发额度时跑突发。额度按放行字节扣、按标准速率回补，
//	         相抵正好是「只扣超出标准的部分」。不拥挤时永远不降速。
//	拥挤     速率 = 调度器本 tick 注给这个池的份额；本 tick 不活跃（没份额）的池封在标准，
//	         不许突发。
//
// 为什么不用 rate.Limiter.WaitN：它的预约是全局 FIFO，一条连接预约走 125ms 的令牌后，
// 同池另一条连接的一个小包也得排在后面——这就是直播卡、起步慢的队头阻塞。这里是
// 「令牌桶深度 = 一个 25ms 量子 + 按连接的起始时间公平排队（SFQ）」：
//
//	量子      桶深 = 当前速率 × 25ms，单次放行半个量子，欠账不超过一个量子
//	公平      每条连接一个 flow，按虚拟起始标签出队；满载连接之间按字节轮转
//	稀疏优先  空闲后回来的连接（含新连接）起始标签 = 当前虚拟时间，可带一个量子的
//	          欠账立即放行——首包不等待，空闲连接 RTT 增量 ≤ 一个量子
//	无全局锁  锁只保护记账，等待走各自的 channel，由一个 AfterFunc 定时器按令牌到期派发
const (
	tierQuantum        = 25 * time.Millisecond
	tierMinQuantumByte = 3000
	tierMaxIdleTTL     = time.Hour
)

// TierPolicy 是一个方向的限速参数，速率单位字节/秒。0 = 该项没有。
//
// Sustained 不参与不拥挤时的整形：它是拥挤时给重度用户的保底（见 node_fairshare.go）。
type TierPolicy struct {
	Standard    uint64
	Burst       uint64
	BurstCredit uint64
	Sustained   uint64
}

func (p TierPolicy) burstOn() bool {
	return p.Standard > 0 && p.Burst > p.Standard && p.BurstCredit > 0
}

// UsesTierShaping 报告这个用户是否有池整形参数。方向标准速率（未配旧的峰值/桶深）
// 或任一突发/持续字段出现即启用；旧 PIR/CIR/CBS 与方向峰值配置走 user_limits.go 原路径。
func (u *MemoryUser) UsesTierShaping() bool {
	if u == nil {
		return false
	}
	if u.BurstBitPerSec != 0 || u.BurstCreditBytes != 0 || u.SustainedBitPerSec != 0 {
		return true
	}
	legacyPeak := u.UploadPeakBps != 0 || u.UploadBurstBytes != 0 || u.DownloadPeakBps != 0 || u.DownloadBurstBytes != 0
	return !legacyPeak && (u.UploadBandwidthBps != 0 || u.DownloadBandwidthBps != 0)
}

// TierPolicies 把用户字段换算成上下行两个方向的整形参数（bit/s → byte/s 只在这里做）。
func (u *MemoryUser) TierPolicies() (up, down TierPolicy) {
	upStd, downStd := u.BandwidthBps, u.BandwidthBps
	if u.UploadBandwidthBps != 0 || u.DownloadBandwidthBps != 0 {
		upStd, downStd = u.UploadBandwidthBps, u.DownloadBandwidthBps
	}
	build := func(std uint64) TierPolicy {
		if std == 0 {
			return TierPolicy{}
		}
		return TierPolicy{
			Standard:    bitsPerSecondToRuntimeBytesPerSecond(std),
			Burst:       bitsPerSecondToRuntimeBytesPerSecond(u.BurstBitPerSec),
			BurstCredit: u.BurstCreditBytes,
			Sustained:   bitsPerSecondToRuntimeBytesPerSecond(u.SustainedBitPerSec),
		}
	}
	return build(upStd), build(downStd)
}

// tierEntry 是一个池。up/down 是执行器；fair 只归调度器 goroutine 读写。
type tierEntry struct {
	up, down *TierShaper
	refs     int
	gen      uint64
	class    string // tierRegistry 锁保护
	fair     poolFairState
}

var tierRegistry = struct {
	sync.Mutex
	m map[string]*tierEntry
}{m: map[string]*tierEntry{}}

// poolKey 是池的身份。显式 pool 优先；前缀让三种来源不会互相撞名。
func poolKey(u *MemoryUser) string {
	switch {
	case u.Pool != "":
		return "pool:" + u.Pool
	case u.Email != "":
		return "email:" + u.Email
	default:
		return fmt.Sprintf("user:%p", u)
	}
}

// HasTierSeam 报告连接是否要挂池整形器。有限速参数、固定出口（egress_tag）、显式池，
// 或节点调度器开着时都挂——没有参数的整形器是直通的，但「不限速→限速」和拥挤时的份额
// 能对已建连接热生效。
func (u *MemoryUser) HasTierSeam() bool {
	return u != nil && (u.UsesTierShaping() || u.EgressTag != "" || u.Pool != "" || nodeFairScheduler.Enabled())
}

// AcquireTierShapers 为一条新连接取该用户所在池的上下行整形器，并以这个用户的当前字段
// 刷新策略（零策略 = 直通）。release 必须在连接结束时调用；不挂整形器时 release 为 nil。
func (u *MemoryUser) AcquireTierShapers() (up, down *TierShaper, release func()) {
	if !u.HasTierSeam() {
		return nil, nil, nil
	}
	upPolicy, downPolicy := u.TierPolicies()
	key := poolKey(u)
	tierRegistry.Lock()
	e := tierRegistry.m[key]
	if e == nil {
		e = &tierEntry{up: newTierShaper(), down: newTierShaper()}
		tierRegistry.m[key] = e
	}
	e.refs++
	e.gen++
	e.class = u.Class
	tierRegistry.Unlock()
	e.up.Configure(upPolicy)
	e.down.Configure(downPolicy)
	nodeFairScheduler.ensureStarted()

	var once sync.Once
	release = func() { once.Do(func() { releaseTierEntry(key, e) }) }
	return e.up, e.down, release
}

// ApplyTierPolicy 把热更后的限速字段推给已建立连接正在用的整形器（改档不断连）。
// 该池当前没有连接时什么都不做——下一条连接建立时会按新字段取整形器。
func ApplyTierPolicy(u *MemoryUser) {
	if u == nil {
		return
	}
	tierRegistry.Lock()
	e := tierRegistry.m[poolKey(u)]
	if e != nil {
		e.class = u.Class
	}
	tierRegistry.Unlock()
	if e == nil {
		return
	}
	upPolicy, downPolicy := u.TierPolicies()
	e.up.Configure(upPolicy)
	e.down.Configure(downPolicy)
}

// 最后一条连接断开后保留池到额度回满、且重度窗口过完为止：否则断开重连就能白拿一整桶
// 突发，或者洗掉自己的重度记录。
func releaseTierEntry(key string, e *tierEntry) {
	tierRegistry.Lock()
	e.refs--
	if e.refs > 0 {
		tierRegistry.Unlock()
		return
	}
	gen, class := e.gen, e.class
	tierRegistry.Unlock()
	ttl := max(e.up.idleTTL(), e.down.idleTTL(), nodeFairScheduler.heavyWindow(class))
	time.AfterFunc(min(ttl, tierMaxIdleTTL), func() {
		tierRegistry.Lock()
		defer tierRegistry.Unlock()
		if tierRegistry.m[key] == e && e.refs == 0 && e.gen == gen {
			delete(tierRegistry.m, key)
		}
	})
}

// TierShaper 是一个池一个方向的整形器。
type TierShaper struct {
	mu     sync.Mutex
	p      TierPolicy
	rate   float64 // 当前速率 byte/s；0 = 不限
	tokens float64 // 可为负：稀疏连接立即放行留下的欠账，最多一个量子
	credit float64
	last   time.Time
	vtime  uint64
	seq    uint64
	queue  tierQueue
	timer  *time.Timer
	armed  bool

	// 调度器写：拥挤时的份额（byte/s）。congested=false 时 share 不起作用。
	congested bool
	share     float64

	// 调度器读：累计放行字节、累计排过队的次数（有排队 = 还想要更多）。
	granted uint64
	waits   uint64
}

func newTierShaper() *TierShaper {
	return &TierShaper{last: time.Now()}
}

// Configure 换策略不换对象：已排队的连接按新速率继续派发。额度只截断不补发，
// 只有从「无突发」切到「有突发」时发一整桶——新池起步即可用突发。
func (s *TierShaper) Configure(p TierPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	old := s.p
	s.p = p
	switch {
	case !p.burstOn():
		s.credit = 0
	case !old.burstOn():
		s.credit = float64(p.BurstCredit)
	default:
		s.credit = min(s.credit, float64(p.BurstCredit))
	}
	s.updateRate()
	s.dispatchLocked()
}

// setShare 由调度器每 tick 调用。congested=false：回到标准/突发；congested=true：
// share>0 时按份额，share=0 时封在标准。
func (s *TierShaper) setShare(congested bool, share uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	s.congested, s.share = congested, float64(share)
	s.updateRate()
	s.dispatchLocked()
}

// sample 给调度器：累计放行字节、累计排队次数、当前策略。
func (s *TierShaper) sample() (granted, waits uint64, p TierPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.granted, s.waits, s.p
}

// Snapshot 返回当前速率（byte/s）与剩余额度，供诊断与测试。
func (s *TierShaper) Snapshot() (rate float64, credit float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	return s.rate, s.credit
}

func (s *TierShaper) idleTTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.p.burstOn() {
		return time.Second
	}
	return time.Duration(float64(s.p.BurstCredit) / float64(s.p.Standard) * float64(time.Second))
}

func quantumBytesAt(rate float64) float64 {
	return max(rate*tierQuantum.Seconds(), tierMinQuantumByte)
}

func (s *TierShaper) quantumBytes() float64 { return quantumBytesAt(s.rate) }

// advance 按上次结算以来的时间回填令牌与额度，并按当下的额度重选速率。
func (s *TierShaper) advance(now time.Time) {
	dt := now.Sub(s.last).Seconds()
	if dt <= 0 {
		return
	}
	s.last = now
	if s.p.burstOn() {
		s.credit = min(float64(s.p.BurstCredit), s.credit+float64(s.p.Standard)*dt)
	}
	if s.rate > 0 {
		s.tokens = min(s.quantumBytes(), s.tokens+s.rate*dt)
	}
	s.updateRate()
}

func (s *TierShaper) rateFor() float64 {
	switch {
	case s.congested && s.share > 0:
		return s.share
	case s.congested || !s.inBurst():
		return float64(s.p.Standard)
	default:
		return float64(s.p.Burst)
	}
}

func (s *TierShaper) inBurst() bool {
	return s.p.burstOn() && s.credit >= float64(s.p.Burst)*tierQuantum.Seconds()
}

func (s *TierShaper) updateRate() {
	rate := s.rateFor()
	if rate == s.rate {
		return
	}
	if s.rate == 0 {
		// 从不限切到限速：发一个量子，已建连接不必先还欠账。
		s.tokens = quantumBytesAt(rate)
	}
	s.rate = rate
	// 降速时上一档的欠账按新速率会超过一个量子，截到一个量子以守住时延上界。
	s.tokens = min(max(s.tokens, -s.quantumBytes()), s.quantumBytes())
}

func (s *TierShaper) grantLocked(start uint64, n int) {
	s.tokens -= float64(n)
	s.granted += uint64(n)
	if s.p.burstOn() {
		s.credit = max(0, s.credit-float64(n))
	}
	s.vtime = max(s.vtime, start)
}

// dispatchLocked 在令牌为正时按起始标签依次放行，放不完就把定时器拨到令牌转正那一刻。
func (s *TierShaper) dispatchLocked() {
	for s.queue.Len() > 0 && (s.rate == 0 || s.tokens > 0) {
		w := heap.Pop(&s.queue).(*tierWaiter)
		s.grantLocked(w.start, w.n)
		close(w.ready)
	}
	if s.queue.Len() == 0 || s.armed {
		return
	}
	delay := time.Duration((1 - s.tokens) / s.rate * float64(time.Second))
	s.armed = true
	if s.timer == nil {
		s.timer = time.AfterFunc(delay, s.onTimer)
	} else {
		s.timer.Reset(delay)
	}
}

func (s *TierShaper) onTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = false
	s.advance(time.Now())
	s.dispatchLocked()
}

// NewFlow 给一条连接的一个方向建公平排队身份。
func (s *TierShaper) NewFlow() *TierFlow {
	if s == nil {
		return nil
	}
	return &TierFlow{s: s}
}

// TierFlow 是一条连接在整形器里的身份；finish 是它的虚拟结束标签。
type TierFlow struct {
	s      *TierShaper
	finish uint64
}

// Wait 阻塞到 n 字节全部被放行。大请求按量子切片，每片单独排队，所以满载连接之间
// 按量子轮转，另一条连接的小包最多等一个量子。
func (f *TierFlow) Wait(ctx context.Context, n int) error {
	s := f.s
	for n > 0 {
		s.mu.Lock()
		s.advance(time.Now())
		if s.rate == 0 {
			// 不限速也记账：调度器要靠它判断节点忙不忙。
			s.granted += uint64(n)
			s.mu.Unlock()
			return nil
		}
		// 半量子切片 + 欠账不超过一个量子：排队者最坏等一个量子，稀疏连接通常立即放行。
		quantum := s.quantumBytes()
		chunk := min(n, int(quantum/2))
		start := max(s.vtime, f.finish)
		f.finish = start + uint64(chunk)
		fresh := start == s.vtime
		if (s.queue.Len() == 0 && s.tokens > 0) || (fresh && s.tokens-float64(chunk) >= -quantum) {
			s.grantLocked(start, chunk)
			s.mu.Unlock()
			n -= chunk
			continue
		}
		s.seq++
		s.waits++
		w := &tierWaiter{start: start, seq: s.seq, n: chunk, ready: make(chan struct{})}
		heap.Push(&s.queue, w)
		s.dispatchLocked()
		s.mu.Unlock()
		select {
		case <-w.ready:
			n -= chunk
		case <-ctx.Done():
			s.mu.Lock()
			if w.index >= 0 {
				heap.Remove(&s.queue, w.index)
			}
			s.mu.Unlock()
			return ctx.Err()
		}
	}
	return nil
}

type tierWaiter struct {
	start, seq uint64
	n          int
	index      int
	ready      chan struct{}
}

type tierQueue []*tierWaiter

func (q tierQueue) Len() int { return len(q) }
func (q tierQueue) Less(i, j int) bool {
	if q[i].start != q[j].start {
		return q[i].start < q[j].start
	}
	return q[i].seq < q[j].seq
}

func (q tierQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}

func (q *tierQueue) Push(x any) {
	w := x.(*tierWaiter)
	w.index = len(*q)
	*q = append(*q, w)
}

func (q *tierQueue) Pop() any {
	old := *q
	w := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	w.index = -1
	return w
}
