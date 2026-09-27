package protocol

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// 三级限速：标准 / 突发（信用）/ 持续，按用户（email）每方向一个整形器。
//
// 为什么不用 rate.Limiter.WaitN：它的预约是全局 FIFO，一条连接预约走 125ms 的令牌后，
// 同用户另一条连接的一个小包也得排在后面——这就是线上直播卡、起步慢的队头阻塞。
// 这里改成「令牌桶深度 = 一个 25ms 量子 + 按连接的起始时间公平排队（SFQ）」：
//
//	量子      桶深 = 当前速率 × 25ms，单次放行半个量子，欠账不超过一个量子
//	公平      每条连接一个 flow，按虚拟起始标签出队；满载连接之间按字节轮转
//	稀疏优先  空闲后回来的连接（含新连接）起始标签 = 当前虚拟时间，可带一个量子的
//	          欠账立即放行——首包不等待，空闲连接 RTT 增量 ≤ 一个量子
//	无全局锁  锁只保护记账，等待走各自的 channel，由一个 AfterFunc 定时器按令牌到期派发
//
// 突发信用只按超出标准的字节扣、低于标准时按差额回补；信用耗尽后仍持续积压满
// sustained_after 秒则降到持续速率，积压消失满 tierRecoverWindow 后恢复。
const (
	tierQuantum        = 25 * time.Millisecond
	tierRecoverWindow  = time.Second
	tierMinQuantumByte = 3000
	tierMaxIdleTTL     = time.Hour
)

// TierPolicy 是一个方向的三级限速参数，速率单位字节/秒。Standard = 0 表示不限。
type TierPolicy struct {
	Standard       uint64
	Burst          uint64
	BurstCredit    uint64
	Sustained      uint64
	SustainedAfter time.Duration
}

func (p TierPolicy) burstOn() bool {
	return p.Standard > 0 && p.Burst > p.Standard && p.BurstCredit > 0
}

func (p TierPolicy) sustainedOn() bool {
	return p.Standard > 0 && p.Sustained > 0 && p.Sustained < p.Standard
}

// UsesTierShaping 报告这个用户是否走三级限速整形器。方向标准速率（未配旧的峰值/桶深）
// 或任一三级字段出现即启用；旧 PIR/CIR/CBS 与方向峰值配置保持原路径不变。
func (u *MemoryUser) UsesTierShaping() bool {
	if u == nil {
		return false
	}
	if u.BurstBitPerSec != 0 || u.BurstCreditBytes != 0 || u.SustainedBitPerSec != 0 || u.SustainedAfterSeconds != 0 {
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
			Standard:       bitsPerSecondToRuntimeBytesPerSecond(std),
			Burst:          bitsPerSecondToRuntimeBytesPerSecond(u.BurstBitPerSec),
			BurstCredit:    u.BurstCreditBytes,
			Sustained:      bitsPerSecondToRuntimeBytesPerSecond(u.SustainedBitPerSec),
			SustainedAfter: time.Duration(u.SustainedAfterSeconds) * time.Second,
		}
	}
	return build(upStd), build(downStd)
}

type tierEntry struct {
	up, down *TierShaper
	refs     int
	gen      uint64
}

var tierRegistry = struct {
	sync.Mutex
	m map[string]*tierEntry
}{m: map[string]*tierEntry{}}

// 同一个逻辑客户挂在多个入站上时 email 相同，共享一套整形器；无 email 的用户按指针隔离。
func tierKey(u *MemoryUser) string {
	if u.Email != "" {
		return u.Email
	}
	return fmt.Sprintf("%p", u)
}

// HasTierSeam 报告连接是否要挂三级整形器。固定出口（egress_tag）的受管用户即使当前
// 不限速也挂一个直通的整形器并放弃 splice，这样「不限速→限速」对已建连接也能热生效。
func (u *MemoryUser) HasTierSeam() bool {
	return u != nil && (u.UsesTierShaping() || u.EgressTag != "")
}

// AcquireTierShapers 为一条新连接取该用户的上下行整形器，并以这个用户的当前字段刷新
// 策略（零策略 = 直通）。release 必须在连接结束时调用；不挂整形器时 release 为 nil。
func (u *MemoryUser) AcquireTierShapers() (up, down *TierShaper, release func()) {
	if !u.HasTierSeam() {
		return nil, nil, nil
	}
	var upPolicy, downPolicy TierPolicy
	if u.UsesTierShaping() {
		upPolicy, downPolicy = u.TierPolicies()
	}
	key := tierKey(u)
	tierRegistry.Lock()
	e := tierRegistry.m[key]
	if e == nil {
		e = &tierEntry{up: newTierShaper(), down: newTierShaper()}
		tierRegistry.m[key] = e
	}
	e.refs++
	e.gen++
	tierRegistry.Unlock()
	e.up.Configure(upPolicy)
	e.down.Configure(downPolicy)

	var once sync.Once
	release = func() { once.Do(func() { releaseTierEntry(key, e) }) }
	return e.up, e.down, release
}

// ApplyTierPolicy 把热更后的限速字段推给已建立连接正在用的整形器（改档不断连）。
// 该用户当前没有连接时什么都不做——下一条连接建立时会按新字段取整形器。
func ApplyTierPolicy(u *MemoryUser) {
	if u == nil {
		return
	}
	tierRegistry.Lock()
	e := tierRegistry.m[tierKey(u)]
	tierRegistry.Unlock()
	if e == nil {
		return
	}
	var upPolicy, downPolicy TierPolicy
	if u.UsesTierShaping() {
		upPolicy, downPolicy = u.TierPolicies()
	}
	e.up.Configure(upPolicy)
	e.down.Configure(downPolicy)
}

// 最后一条连接断开后保留状态到信用回满为止：否则断开重连就能白拿一整桶突发。
func releaseTierEntry(key string, e *tierEntry) {
	tierRegistry.Lock()
	e.refs--
	if e.refs > 0 {
		tierRegistry.Unlock()
		return
	}
	gen := e.gen
	tierRegistry.Unlock()
	time.AfterFunc(max(e.up.idleTTL(), e.down.idleTTL()), func() {
		tierRegistry.Lock()
		defer tierRegistry.Unlock()
		if tierRegistry.m[key] == e && e.refs == 0 && e.gen == gen {
			delete(tierRegistry.m, key)
		}
	})
}

// TierShaper 是一个用户一个方向的整形器。
type TierShaper struct {
	mu        sync.Mutex
	p         TierPolicy
	rate      float64 // 当前档速率 byte/s；0 = 不限
	tokens    float64 // 可为负：稀疏连接立即放行留下的欠账，最多一个量子
	credit    float64
	last      time.Time
	vtime     uint64
	seq       uint64
	queue     tierQueue
	timer     *time.Timer
	armed     bool
	satSince  time.Time
	lastBusy  time.Time
	sustained bool
}

func newTierShaper() *TierShaper {
	return &TierShaper{last: time.Now()}
}

// Configure 换策略不换对象：已排队的连接按新速率继续派发。信用只截断不补发，
// 只有从「无突发」切到「有突发」时发一整桶。
func (s *TierShaper) Configure(p TierPolicy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.advance(now)
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
	if old.Standard == 0 {
		s.tokens = s.quantumBytesAt(s.rateFor(now))
	}
	s.updateTier(now)
	s.tokens = min(s.tokens, s.quantumBytes())
	s.dispatchLocked()
}

// Snapshot 返回当前档速率（byte/s）与剩余信用，供诊断与测试。
func (s *TierShaper) Snapshot() (rate float64, credit float64, sustained bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	return s.rate, s.credit, s.sustained
}

func (s *TierShaper) idleTTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	ttl := tierRecoverWindow
	if s.p.burstOn() {
		ttl = max(ttl, time.Duration(float64(s.p.BurstCredit)/float64(s.p.Standard)*float64(time.Second)))
	}
	return min(ttl, tierMaxIdleTTL)
}

func (s *TierShaper) quantumBytesAt(rate float64) float64 {
	return max(rate*tierQuantum.Seconds(), tierMinQuantumByte)
}

func (s *TierShaper) quantumBytes() float64 { return s.quantumBytesAt(s.rate) }

// advance 按上次结算以来的时间回填令牌与信用。信用按标准速率回补，放行时按全量扣，
// 两者相抵正好是「只扣超出标准的部分」。持续档期间信用冻结，避免在两档之间来回跳。
func (s *TierShaper) advance(now time.Time) {
	dt := now.Sub(s.last).Seconds()
	if dt <= 0 {
		return
	}
	s.last = now
	if s.p.burstOn() && !s.sustained {
		s.credit = min(float64(s.p.BurstCredit), s.credit+float64(s.p.Standard)*dt)
	}
	if s.rate > 0 {
		s.tokens = min(s.quantumBytes(), s.tokens+s.rate*dt)
	}
	s.updateTier(now)
}

func (s *TierShaper) rateFor(now time.Time) float64 {
	switch {
	case s.p.Standard == 0:
		return 0
	case s.sustained:
		return float64(s.p.Sustained)
	case s.inBurst():
		return float64(s.p.Burst)
	default:
		return float64(s.p.Standard)
	}
}

func (s *TierShaper) inBurst() bool {
	return s.p.burstOn() && !s.sustained && s.credit >= float64(s.p.Burst)*tierQuantum.Seconds()
}

func (s *TierShaper) updateTier(now time.Time) {
	// 信用在阈值附近抖动造成的瞬时突发不算「负载降下来」，只有积压消失满恢复窗口才清零。
	if s.queue.Len() > 0 {
		s.lastBusy = now
		if s.satSince.IsZero() && !s.inBurst() {
			s.satSince = now
		}
	}
	if now.Sub(s.lastBusy) >= tierRecoverWindow {
		s.satSince, s.sustained = time.Time{}, false
	}
	if s.p.sustainedOn() && !s.satSince.IsZero() && now.Sub(s.satSince) >= s.p.SustainedAfter {
		s.sustained = true
	}
	if !s.p.sustainedOn() {
		s.sustained = false
	}
	if rate := s.rateFor(now); rate != s.rate {
		// 降档时上一档的欠账按新速率还会超过一个量子，截到一个量子以守住时延上界。
		s.rate = rate
		s.tokens = max(s.tokens, -s.quantumBytes())
	}
}

func (s *TierShaper) grantLocked(start uint64, n int) {
	s.tokens -= float64(n)
	if s.p.burstOn() && !s.sustained {
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
		now := time.Now()
		s.advance(now)
		if s.rate == 0 {
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
		w := &tierWaiter{start: start, seq: s.seq, n: chunk, ready: make(chan struct{})}
		heap.Push(&s.queue, w)
		s.updateTier(now)
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
