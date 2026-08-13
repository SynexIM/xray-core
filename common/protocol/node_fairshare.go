package protocol

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// NodeFairScheduler 是【节点级工作保全最大最小公平】带宽调度器（ipipx 魔改）。
//
// 设计（research/fair-bandwidth-design.md 方案 b：周期重算）：
//   - 节点总出口上限 availBps = node_total × headroom（后端下发，SetNodeBandwidth 原子更新）。
//   - 后台 goroutine 每 Δt 算 share = availBps / len(active)，对每个活跃 email 把其
//     上/下行限速器速率设为 min(own_limit, share)；非活跃 email 还原 own_limit（或不限）。
//   - 活跃判定走字节增量（限速 wrapper 自己累加，无需耦合 stats）。
//
// 与 per-user 限速共存：节点公平是【正交的第二层桶】，套在 per-user 桶之外（双向）。
// 这样天然处理两个 caveat：
//   - unlimited 用户（BandwidthBps==0）：per-user 桶为 nil，但节点公平仍给它挂桶纳入公平，
//     own_limit 视为 ∞ → eff = share，不会绕过节点公平打满节点。
//   - 双向出口：per-user 限速只在 uplink(Reader)；节点公平 Reader+Writer 都挂，
//     使「节点总出口」按双向合计语义生效。
//
// 热路径：限速 wrapper 的读/写各过一次 WaitN（per-email limiter 锁，非全局锁）+ 一次原子累加。
// recompute 在后台 1s 一次，遍历 members 调 SetLimit（per-limiter 锁），不在转发热路径。
type NodeFairScheduler struct {
	mu       sync.Mutex             // 保护 members 与 availBps 的复合读写（recompute / Register / SetNodeBandwidth）
	availBps atomic.Uint64          // = node_total × headroom（字节/秒）；0=不开节点级公平
	members  map[string]*fairMember // email → 成员（同 email 跨连接共享一组桶）

	softFloorBps atomic.Uint64 // 公平软地板（字节/秒）；0=默认 fairSoftFloorDefaultB
	hardFloorBps atomic.Uint64 // 绝对硬地板（字节/秒）；0=默认 fairHardFloorDefaultB

	started atomic.Bool // 后台 recompute goroutine 是否已启动（懒启动，首次 Register 时拉起）
}

// fairMember 是一个 email 在节点公平里的成员态。同一 email 的所有连接共享 up/down 两个桶。
type fairMember struct {
	user *MemoryUser // own_limit = user.BandwidthBps(bit/s) 转 runtime byte/s（实时读，UpdateUser 改限速即生效）

	upLimiter   *rate.Limiter // 上行（Reader）公平桶
	downLimiter *rate.Limiter // 下行（Writer）公平桶

	conns     atomic.Int64  // 当前活跃连接数（惰性清理与观测用）
	bytes     atomic.Uint64 // 累计经过字节（双向合计），活跃判定用
	lastBytes uint64        // 上轮 recompute 时的 bytes 快照（仅 recompute goroutine 访问，mu 保护）

	// 活跃滞回状态（仅 recompute 访问，mu 保护）：防 share↔own_limit 每秒震荡。
	active    bool // 当前是否活跃（进入阈值 4KB/tick，退出需连续 3 tick < 1KB）
	idleTicks int  // 活跃态下连续低于退出阈值的 tick 数
	zeroTicks int  // 连续「零字节且零连接」tick 数（惰性清理判据）
}

var nodeFairScheduler = &NodeFairScheduler{members: make(map[string]*fairMember)}

// FairScheduler 返回进程级单例（节点 = 单 xray 进程）。
func FairScheduler() *NodeFairScheduler { return nodeFairScheduler }

const (
	fairRecomputeEvery = time.Second

	// 活跃判定滞回：进入活跃阈值 > 4KB/tick（滤 keepalive）；退出需增量 < 1KB
	// 且连续 3 tick。中间带 [1KB, 4KB] 保持原状态。目标：间歇流量（网页浏览）
	// 用户的限速值不在 share↔own_limit 之间每秒跳变。
	fairActiveEnterDeltaB = 4 * 1024
	fairActiveExitDeltaB  = 1 * 1024
	fairActiveExitTicks   = 3

	// 软地板：正常拥挤时每活跃用户软保证 0.5Mbps（÷8 = 62_500 字节/秒）。
	// 均分调度下 share=avail/N ≥ soft 当且仅当 soft×N ≤ avail，故软地板在该区间
	// 是护栏；soft×N > avail（活跃用户多到给不起）时退回物理均分 + 硬地板兜底。
	fairSoftFloorDefaultB = 500_000 / 8
	// 绝对硬地板 16KB/s：任何情况下公平桶不把用户掐到该值以下——只慢不断连，
	// 网页仍能打开。per-user own 桶独立执行购买限速，这里抬地板不会突破 own。
	fairHardFloorDefaultB = 16 * 1024

	// 惰性清理：连续 10 分钟（600 tick）零字节且零连接的成员移除。
	// members 以 email 为键，RemoveUser 路径只有 *MemoryUser（各协议 validator 各
	// 自实现），无法像 runtimeLimiters 那样按指针精确删除；惰性清理与
	// ResetRuntimeConnections 的「不留陈旧运行态」目标一致且不侵入各协议。
	fairMemberExpireTicks = 600

	// burst 下限 = 单次读缓冲（buf.Size=8KB；不 import buf 避免包环）。
	fairBurstFloorB = 8 * 1024
)

// SetNodeBandwidth 设置节点总出口上限（字节/秒，已含 headroom 折算）。0=关闭节点级公平。
// 由 node-agent 收到 NodeConfig 后经 SetNodeBandwidth command API 调用。
func (s *NodeFairScheduler) SetNodeBandwidth(availBps uint64) {
	s.availBps.Store(availBps)
}

// AvailBps 返回当前调度顶（测试/观测用）。
func (s *NodeFairScheduler) AvailBps() uint64 { return s.availBps.Load() }

// Enabled 节点级公平是否开启（availBps>0）。
func (s *NodeFairScheduler) Enabled() bool { return s.availBps.Load() > 0 }

// fairBurst 由速率推 burst（与 buf.rateLimitBurst 同心智）：1/8 秒配额（125ms 窗口），
// 整秒 burst 会造成 1s 突发 + 1s 静默的锯齿；floor 到单次读缓冲防小速率下过碎。
func fairBurst(bps uint64) int {
	b := int(bps / 8)
	if b < fairBurstFloorB {
		b = fairBurstFloorB
	}
	return b
}

// SetFloors 覆盖软/硬地板（字节/秒），0=用默认值。经 FairShareService.SetNodeBandwidth
// 配置链路随节点带宽一并下发（旧 node-agent 不带该字段 → 0 → 默认值，向后兼容）。
func (s *NodeFairScheduler) SetFloors(softBps, hardBps uint64) {
	s.softFloorBps.Store(softBps)
	s.hardFloorBps.Store(hardBps)
}

// floors 返回生效的软/硬地板（防配置倒挂：hard 不高于 soft）。
func (s *NodeFairScheduler) floors() (soft, hard uint64) {
	soft = s.softFloorBps.Load()
	if soft == 0 {
		soft = fairSoftFloorDefaultB
	}
	hard = s.hardFloorBps.Load()
	if hard == 0 {
		hard = fairHardFloorDefaultB
	}
	if hard > soft {
		hard = soft
	}
	return soft, hard
}

func fairOwnLimitBytesPerSecond(user *MemoryUser) uint64 {
	if user == nil || user.BandwidthBps == 0 {
		return 0
	}
	return bitsPerSecondToRuntimeBytesPerSecond(user.BandwidthBps)
}

// Member 取（或懒建）某 user 的公平成员，返回上/下行桶。节点公平未开启时返回 (nil,nil)
// —— 调用方据此不挂 wrapper，零开销。首次注册时拉起后台 recompute goroutine。
//
// 用 email 作 key（非 *MemoryUser 指针）：UpdateUser 会换 *MemoryUser 实例，但 email 稳定，
// 桶应跨实例存活。member.user 指针每次 Register 刷新为最新，own_limit 实时读最新值。
func (s *NodeFairScheduler) Member(user *MemoryUser) (up, down *rate.Limiter) {
	if user == nil || len(user.Email) == 0 || !s.Enabled() {
		return nil, nil
	}
	s.ensureStarted()

	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[user.Email]
	if m == nil {
		// 初始速率：min(own, avail)。own=0(unlimited) → avail。下一轮 recompute 收敛到 share。
		init := s.availBps.Load()
		if own := fairOwnLimitBytesPerSecond(user); own > 0 && own < init {
			init = own
		}
		burst := fairBurst(init)
		m = &fairMember{
			user:        user,
			upLimiter:   rate.NewLimiter(rate.Limit(init), burst),
			downLimiter: rate.NewLimiter(rate.Limit(init), burst),
		}
		s.members[user.Email] = m
	} else {
		m.user = user // 刷新为最新 MemoryUser（UpdateUser 后 own_limit 实时生效）
	}
	return m.upLimiter, m.downLimiter
}

// addBytes 由限速 wrapper 调用，累加经过字节（活跃判定）。
func (m *fairMember) addBytes(n int) { m.bytes.Add(uint64(n)) }

// Acquire 取某 user 的节点公平桶 + 字节回调 + 连接释放函数（一连接一次，在 dispatcher 热路径调）。
// 节点公平未开启或无 email 时返回 (nil,nil,nil,noop)，调用方据此不挂 wrapper（零开销）。
//
//	up/down  上/下行公平限速器（速率与 burst 由后台 recompute 周期重算，wrapper 动态读）
//	onBytes  字节回调（活跃判定）；wrapper 每次读/写后调
//	release  连接结束时调（连接计数 -1），挂 context.AfterFunc
func (s *NodeFairScheduler) Acquire(user *MemoryUser) (up, down *rate.Limiter, onBytes func(int), release func()) {
	u, d := s.Member(user)
	if u == nil {
		return nil, nil, nil, func() {}
	}
	email := user.Email
	s.connOpened(email)
	s.mu.Lock()
	m := s.members[email]
	s.mu.Unlock()
	onBytes = func(n int) {
		if m != nil {
			m.addBytes(n)
		}
	}
	release = func() { s.connClosed(email) }
	return u, d, onBytes, release
}

// connOpened / connClosed 维护活跃连接计数（观测；清理常驻成员的依据，当前不主动清理）。
func (s *NodeFairScheduler) connOpened(email string) {
	s.mu.Lock()
	if m := s.members[email]; m != nil {
		m.conns.Add(1)
	}
	s.mu.Unlock()
}

func (s *NodeFairScheduler) connClosed(email string) {
	s.mu.Lock()
	if m := s.members[email]; m != nil {
		m.conns.Add(-1)
	}
	s.mu.Unlock()
}

// ensureStarted 懒启动后台 recompute goroutine（一次性）。
func (s *NodeFairScheduler) ensureStarted() {
	if s.started.CompareAndSwap(false, true) {
		go s.run()
	}
}

func (s *NodeFairScheduler) run() {
	t := time.NewTicker(fairRecomputeEvery)
	defer t.Stop()
	for range t.C {
		s.recompute()
	}
}

// recompute 周期重算每 email 的有效速率（均分版 max-min，满足「大概即可」）。
//
//	avail := availBps
//	active := 滞回判定（进 4KB/tick、退 <1KB 连续 3 tick）
//	share := fairShare(avail, len(active))   // 软地板 0.5Mbps 护栏 + 绝对硬地板 16KB/s
//	活跃: SetLimit(max(min(own_or_inf, share), hard))；非活跃: 还原 own_limit
//	（own=0 → 还原到 avail，不压制）。顺带惰性清理长期无流量无连接的成员。
func (s *NodeFairScheduler) recompute() {
	avail := s.availBps.Load()

	s.mu.Lock()
	defer s.mu.Unlock()
	if avail == 0 || len(s.members) == 0 {
		return
	}

	active := make([]*fairMember, 0, len(s.members))
	for email, m := range s.members {
		cur := m.bytes.Load()
		delta := cur - m.lastBytes
		m.lastBytes = cur

		// 惰性清理：连续 fairMemberExpireTicks 轮零字节且零连接 → 移除
		//（conns==0 保证没有存活 wrapper 引用它的桶；再来连接会经 Member 重建）。
		if delta == 0 && m.conns.Load() == 0 {
			m.zeroTicks++
			if m.zeroTicks >= fairMemberExpireTicks {
				delete(s.members, email)
				continue
			}
		} else {
			m.zeroTicks = 0
		}

		// 活跃滞回：中间带 [exit, enter] 保持原状态，退出需连续 N tick 低于阈值。
		if m.active {
			if delta < fairActiveExitDeltaB {
				m.idleTicks++
				if m.idleTicks >= fairActiveExitTicks {
					m.active = false
					m.idleTicks = 0
				}
			} else {
				m.idleTicks = 0
			}
		} else if delta > fairActiveEnterDeltaB {
			m.active = true
			m.idleTicks = 0
		}
		if m.active {
			active = append(active, m)
		}
	}

	if len(active) == 0 {
		// 无人跑流量：所有成员还原个人上限（不被份额压制）。
		for _, m := range s.members {
			s.applyOwn(m, avail)
		}
		return
	}

	share := s.fairShare(avail, uint64(len(active)))
	_, hard := s.floors()
	for _, m := range s.members {
		if m.active {
			eff := share
			if own := fairOwnLimitBytesPerSecond(m.user); own > 0 && own < eff {
				eff = own
			}
			// 绝对硬地板：公平桶任何情况下不把用户掐到 hard 以下（只慢不断连）。
			// own 限速由独立的 per-user 桶执行，这里抬地板不会让用户超过购买限速。
			if eff < hard {
				eff = hard
			}
			s.setLimit(m, eff)
		} else {
			s.applyOwn(m, avail)
		}
	}
}

// fairShare 计算活跃用户份额（字节/秒）：
//   - soft×n ≤ avail：均分份额天然 ≥ 软地板（0.5Mbps），soft 仅作护栏显式夹一次。
//   - soft×n > avail：活跃用户多到软地板给不起 → 按物理份额均分，绝对硬地板兜底
//     （替代旧的 share==0→1 字节/秒：1B/s 等于事实断连，违反「只慢不断连」）。
func (s *NodeFairScheduler) fairShare(avail, n uint64) uint64 {
	soft, hard := s.floors()
	share := avail / n
	if soft*n <= avail {
		if share < soft {
			share = soft
		}
	} else if share < hard {
		share = hard
	}
	return share
}

// applyOwn 把成员还原到个人上限（own=0 unlimited → 还原到 avail，不压制但仍受节点顶约束）。
func (s *NodeFairScheduler) applyOwn(m *fairMember, avail uint64) {
	own := avail
	if userOwn := fairOwnLimitBytesPerSecond(m.user); userOwn > 0 && userOwn < own {
		own = userOwn
	}
	s.setLimit(m, own)
}

// setLimit 同步更新速率与 burst（burst=1/8 秒配额）。burst 不随速率缩小会导致低速
// 时段积累整秒级令牌 → 锯齿。wrapper 侧每轮 WaitN 动态读 Burst() 并对并发缩小重试，
// 因此这里可以安全 SetBurst。
func (s *NodeFairScheduler) setLimit(m *fairMember, bps uint64) {
	lim := rate.Limit(bps)
	burst := fairBurst(bps)
	m.upLimiter.SetLimit(lim)
	m.upLimiter.SetBurst(burst)
	m.downLimiter.SetLimit(lim)
	m.downLimiter.SetBurst(burst)
}
