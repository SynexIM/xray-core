package protocol

import (
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// NodeFairScheduler 是节点级【自适应带宽调度器】：work-conserving 加权 max-min 公平
// （注水法），对标运营商 BNG 里的 Subscriber-aware Hierarchical QoS。
//
// 一句话：每秒看一眼谁真的还想要更多，把节点出口按权重注给他们，谁也不预留。
//
//	每 tick 把活跃成员分两类
//	  satisfied   本 tick 从未因等令牌阻塞  →  他就要这么多，demand = 实测吞吐
//	  backlogged  阻塞过                    →  他还想要更多，具体多少不必猜
//	分配
//	  1  地板先扣掉（前提：地板×活跃人数 ≤ root_cap，给不起就不给）
//	  2  satisfied 按实测吞吐（+ 一点余量）钉住，扣掉
//	  3  剩下的池子在 backlogged 里按 weight 分
//	  4  谁分到超过自己天花板就钉住、多的还池，回 3 再分，直到无人新饱和
//
// 与旧实现（share = avail / len(active) 纯人头均分）的三个关键区别：
//
//	① 只想要 0.17 Mbps 的人不再占着一整份 —— 剩余带宽真的流向高需求者（FR-073）。
//	② 地板有前提。旧实现无条件把每个人抬到硬地板，节点最挤时发出的额度可达 root_cap
//	   的 5 倍，队列排到上游运营商缓冲区里去，节点级公平在最需要它的时刻完全失效。
//	   主人 2026-08-22 裁定：选「节点总出口守得住」（FR-077）。
//	③ 不拥挤时根本不削速（FR-076），越过上阈值才进公平模式，滞回退出。
//
// 额度总量契约（完成定义 #4）：**只要调度器进入约束态**——即有成员被权重份额压住，
// 而不是拿到自己想要的全部——Σ allocation ≤ root_cap，一个字节都不多发。
// 反过来，没人被压住时（大家加起来都没要满）每人发的是各自天花板，合计可以大于
// root_cap：那不是超发，那是 work-conserving 的定义，因为没人真的想要那么多。
//
// 与 per-user 限速共存：节点公平是【正交的第二层桶】，套在 per-user 桶之外（双向）。
//   - unlimited 用户（BandwidthBps==0）：per-user 桶为 nil，节点公平仍给它挂桶纳入公平。
//   - 双向出口：per-user 限速只在 uplink(Reader)；节点公平 Reader+Writer 都挂。
//
// 热路径：限速 wrapper 的读/写各过一次 WaitN（per-email limiter 锁，非全局锁）+ 两次
// 原子累加。recompute 在后台 1s 一次，不在转发热路径。
type NodeFairScheduler struct {
	mu      sync.Mutex             // 保护 members / 拥塞状态的复合读写
	members map[string]*fairMember // email → 成员（同 email 跨连接共享一组桶）

	// root_cap：节点整形上限（字节/秒，已含 headroom）。0 = 不开节点级公平。
	// ⚠️ 字节/秒。MemoryUser.BandwidthBps 是比特/秒，差 8 倍，见 node_fairshare_units_test.go。
	rootCapBytePerSec atomic.Uint64

	// 地板（字节/秒）。0 = 无地板 —— **不是「用默认值」**（FR-079c：不许有默认带宽）。
	softFloorBytePerSec atomic.Uint64
	hardFloorBytePerSec atomic.Uint64

	// 拥塞滞回（FR-076）。enter=0 表示不做拥塞判定、永远公平模式。
	congestionEnterPercent atomic.Uint32
	congestionExitPercent  atomic.Uint32
	congestionExitTicks    atomic.Uint32

	// class 策略表，copy-on-write（读端无锁，recompute 每 tick 读一次）。
	classes atomic.Pointer[map[string]*ClassPolicy]

	congested      bool // mu
	belowExitTicks int  // mu

	started atomic.Bool // 后台 recompute goroutine 是否已启动（懒启动）
}

// ClassPolicy 是一个 class（= 一个 SKU）的争抢策略，随 SetClassPolicy 整份下发。
type ClassPolicy struct {
	Name   string
	Weight uint32 // 0 视为 1

	// NormalCapBytePerSec 是「不挤的时候你能一直跑到这么快」，**不是保证带宽/CIR**
	// （FR-071：500 客户 × 20Mbps = 10Gbps，物理上不可能承诺）。0 = 无 class 级上限。
	NormalCapBytePerSec uint64

	// BurstCapBytePerSec / BurstCreditBytes 见 burst_credit.go。
	BurstCapBytePerSec uint64
	BurstCreditBytes   uint64

	// FloorRatioPercent：该 class 的地板 = NormalCapBytePerSec × 百分比。0 = 无专属地板。
	FloorRatioPercent uint32
}

// fairMember 是一个 email 在节点公平里的成员态。同一 email 的所有连接共享 up/down 两个桶。
type fairMember struct {
	user *MemoryUser // 天花板实时读（UpdateUser 改限速即生效）

	upLimiter   *rate.Limiter // 上行（Reader）公平桶
	downLimiter *rate.Limiter // 下行（Writer）公平桶

	conns   atomic.Int64  // 当前活跃连接数
	bytes   atomic.Uint64 // 累计经过字节（双向合计），需求测量与活跃判定用
	blocked atomic.Uint64 // 累计「因等令牌而阻塞」次数 —— backlogged 判据（FR-074）

	// 以下字段仅 recompute goroutine 访问，mu 保护。
	lastBytes   uint64
	lastBlocked uint64
	lastDelta   uint64 // 上一 tick 实测吞吐（字节/秒，tick=1s）

	active    bool
	idleTicks int
	zeroTicks int

	credit burstCredit // 突发信用（burst_credit.go）

	// 每轮注水的临时量（不跨 tick 存活，放这里只为省 map 分配）。
	backlogged bool
	weight     uint64
	ceiling    uint64
	floor      uint64
	want       uint64
	alloc      uint64
	pinned     bool
}

var nodeFairScheduler = &NodeFairScheduler{members: make(map[string]*fairMember)}

// FairScheduler 返回进程级单例（节点 = 单 xray 进程）。
func FairScheduler() *NodeFairScheduler { return nodeFairScheduler }

const (
	fairRecomputeEvery     = time.Second
	fairRecomputeEveryMsec = 1000

	// 活跃判定滞回：进入活跃阈值 > 4KB/tick（滤 keepalive）；退出需增量 < 1KB
	// 且连续 3 tick。中间带 [1KB, 4KB] 保持原状态。
	fairActiveEnterDeltaB = 4 * 1024
	fairActiveExitDeltaB  = 1 * 1024
	fairActiveExitTicks   = 3

	// satisfied 成员的余量：把「他刚才跑了多少」当需求，直接钉在那个数上会让他下一
	// tick 立刻撞桶变成 backlogged，再下一 tick 又变回 satisfied —— 每秒来回抖。
	// 给 1/8 的抬头让他稳住，同时不至于把带宽虚占给用不掉的人。
	fairSatisfiedHeadroomDiv = 8

	// 注水轮数上限。正常两三轮收敛（每轮至少钉住一个成员）；成员极多且天花板各不
	// 相同时最坏是 O(N) 轮，5 万成员会把 1 秒的 tick 跑穿。截断后把余下的池子按权重
	// 一次分完 —— 少数几个本可以再钉住的成员分到略多一点，公平性误差远小于跑不完。
	fairFillMaxRounds = 8

	// 惰性清理：连续 10 分钟（600 tick）零字节且零连接的成员移除。
	fairMemberExpireTicks = 600

	// burst 下限 = 单次读缓冲（buf.Size=8KB；不 import buf 避免包环）。
	fairBurstFloorB = 8 * 1024
)

// SetNodeBandwidth 设置 root_cap（字节/秒，已含 headroom 折算）。0=关闭节点级公平。
func (s *NodeFairScheduler) SetNodeBandwidth(rootCapBytePerSec uint64) {
	s.rootCapBytePerSec.Store(rootCapBytePerSec)
}

// RootCapBytePerSec 返回当前节点整形上限（测试/观测用）。
func (s *NodeFairScheduler) RootCapBytePerSec() uint64 { return s.rootCapBytePerSec.Load() }

// Enabled 节点级公平是否开启（root_cap > 0）。
func (s *NodeFairScheduler) Enabled() bool { return s.rootCapBytePerSec.Load() > 0 }

// SetFloors 设置软/硬地板（字节/秒）。**0 = 无地板**，不是「用默认值」（FR-079c）。
// 地板本身还有前提：地板 × 活跃人数 ≤ root_cap 才生效，给不起就不给（FR-077）。
func (s *NodeFairScheduler) SetFloors(softBytePerSec, hardBytePerSec uint64) {
	s.softFloorBytePerSec.Store(softBytePerSec)
	s.hardFloorBytePerSec.Store(hardBytePerSec)
}

// FloorsBytePerSec 返回当前生效的软/硬地板（观测与测试用）。
// 返回 0 就是真的没有地板 —— 这个读口存在的意义之一，就是让「0 有没有被偷偷换成
// 默认值」这件事可以被断言。
func (s *NodeFairScheduler) FloorsBytePerSec() (soft, hard uint64) {
	return s.softFloorBytePerSec.Load(), s.hardFloorBytePerSec.Load()
}

// SetCongestionHysteresis 设置拥塞进出阈值（百分比）与退出所需的连续 tick 数。
// enterPercent = 0 表示不做拥塞判定：永远处于公平模式（改造前的行为）。
func (s *NodeFairScheduler) SetCongestionHysteresis(enterPercent, exitPercent, exitTicks uint32) {
	s.congestionEnterPercent.Store(enterPercent)
	s.congestionExitPercent.Store(exitPercent)
	s.congestionExitTicks.Store(exitTicks)
}

// SetClassPolicies 整份替换 class 策略表（声明式，与面板下发同心智）。
// copy-on-write：换指针，读端不加锁。
func (s *NodeFairScheduler) SetClassPolicies(policies []*ClassPolicy) {
	tbl := make(map[string]*ClassPolicy, len(policies))
	for _, p := range policies {
		if p == nil {
			continue
		}
		cp := *p
		tbl[cp.Name] = &cp
	}
	s.classes.Store(&tbl)
}

// ClassPolicyFor 返回某 class 名生效的策略：先精确匹配，再落到名字为空的兜底策略。
// 都没有则返回 nil（同权重、无 class 上限、无突发）。
func (s *NodeFairScheduler) ClassPolicyFor(name string) *ClassPolicy {
	tbl := s.classes.Load()
	if tbl == nil {
		return nil
	}
	if p := (*tbl)[name]; p != nil {
		return p
	}
	if name == "" {
		return nil
	}
	return (*tbl)[""]
}

// fairOwnLimitBytesPerSecond 返回这个用户的【实际天花板】（字节/秒），
// 0 = 他自己没有天花板（调用点据此当作 ∞，只受份额与 class 约束）。
//
// 为什么不能只看 BandwidthBps：双速率语义里「PIR = 0 且设了 CIR」等于单速率 CIR
// （见 RuntimeRateLimiters）。只读 BandwidthBps 的话，这种用户会被当成不限速，
// 拥挤时分到他根本跑不满的份额，节点容量白白空转。
//
// 实际天花板 = 他能跑到的最快速度：有 PIR 时是 PIR（CIR 只在 CBS 花完后才压更低，
// 那是长期均值，不是天花板），没有 PIR 时 CIR 就是天花板。
func fairOwnLimitBytesPerSecond(user *MemoryUser) uint64 {
	if user == nil {
		return 0
	}
	ceiling := user.BandwidthBps
	if ceiling == 0 {
		ceiling = user.CommittedBps
	}
	return bitsPerSecondToRuntimeBytesPerSecond(ceiling)
}

// Member 取（或懒建）某 user 的公平成员，返回上/下行桶。节点公平未开启时返回 (nil,nil)。
//
// 用 email 作 key（非 *MemoryUser 指针）：UpdateUser 会换 *MemoryUser 实例，但 email 稳定。
func (s *NodeFairScheduler) Member(user *MemoryUser) (up, down *rate.Limiter) {
	if user == nil || len(user.Email) == 0 || !s.Enabled() {
		return nil, nil
	}
	s.ensureStarted()

	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.members[user.Email]
	if m == nil {
		m = &fairMember{user: user}
		// 先发一桶突发信用再算初始速率，否则新客户的头一秒永远跑基准 ——
		// 「打开网页觉得很快」正好发生在那一秒里。
		m.credit.settle(s.ClassPolicyFor(m.className()), 0, 0)
		// 初始速率 = 他自己的天花板，下一轮 recompute 才可能按拥塞压低。
		init := s.ceilingFor(m, s.rootCapBytePerSec.Load())
		m.upLimiter = rate.NewLimiter(rate.Limit(init), s.burstFor(m, init))
		m.downLimiter = rate.NewLimiter(rate.Limit(init), s.burstFor(m, init))
		s.members[user.Email] = m
	} else {
		m.user = user // 刷新为最新 MemoryUser
	}
	return m.upLimiter, m.downLimiter
}

// FairHooks 是一条连接挂节点公平所需的全部东西。Acquire 返回 nil 表示节点公平
// 未开启或该用户不参与，调用方据此不挂 wrapper（零开销）。
type FairHooks struct {
	Up   *rate.Limiter // 上行（Reader）
	Down *rate.Limiter // 下行（Writer）

	// OnBytes 每次读/写后调，累加经过字节：既是活跃判定，也是 satisfied 的需求测量。
	OnBytes func(n int)

	// OnBlocked 在一次取令牌确实等待过之后调 —— 这就是 backlogged 信号（FR-074）：
	// 阻塞过 = 他还想要更多。不猜数值、不做 DPI、不做流量指纹。
	OnBlocked func(waited time.Duration, n int)

	// Release 连接结束时调，挂 context.AfterFunc。
	Release func()
}

// Acquire 取某 user 的节点公平挂载（一连接一次，在 dispatcher 热路径调）。
func (s *NodeFairScheduler) Acquire(user *MemoryUser) *FairHooks {
	up, down := s.Member(user)
	if up == nil {
		return nil
	}
	email := user.Email
	s.mu.Lock()
	m := s.members[email]
	if m != nil {
		m.conns.Add(1)
	}
	s.mu.Unlock()
	if m == nil {
		return nil
	}
	return &FairHooks{
		Up:        up,
		Down:      down,
		OnBytes:   func(n int) { m.bytes.Add(uint64(n)) },
		OnBlocked: func(time.Duration, int) { m.blocked.Add(1) },
		Release:   func() { m.conns.Add(-1) },
	}
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

// recompute 是每 tick 的全部工作：采样 → 拥塞判定 → 注水 → 落桶。
func (s *NodeFairScheduler) recompute() {
	root := s.rootCapBytePerSec.Load()

	s.mu.Lock()
	defer s.mu.Unlock()
	if root == 0 || len(s.members) == 0 {
		return
	}

	active := make([]*fairMember, 0, len(s.members))
	var used uint64
	for email, m := range s.members {
		cur := m.bytes.Load()
		delta := cur - m.lastBytes
		m.lastBytes = cur
		m.lastDelta = delta
		used += delta

		blk := m.blocked.Load()
		m.backlogged = blk != m.lastBlocked
		m.lastBlocked = blk

		// 突发信用结算：所有成员都结（空闲的人正是要回补信用的人）。
		m.credit.settle(s.ClassPolicyFor(m.className()), delta, fairRecomputeEveryMsec)

		// 惰性清理：连续 fairMemberExpireTicks 轮零字节且零连接 → 移除。
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

	// 拥塞判定要在活跃统计之后：利用率算的是全体成员的实际吞吐 / root_cap。
	congested := s.updateCongestion(used, root)

	if len(active) == 0 || !congested {
		// 不挤就不削速（FR-076）：每人跑自己的天花板。
		for _, m := range s.members {
			s.setLimit(m, s.ceilingFor(m, root))
		}
		return
	}

	s.fill(active, root)
	for _, m := range s.members {
		if m.active {
			s.setLimit(m, m.alloc)
		} else {
			s.setLimit(m, s.ceilingFor(m, root))
		}
	}
}

func (m *fairMember) className() string {
	if m.user == nil {
		return ""
	}
	return m.user.Class
}

// updateCongestion 维护拥塞滞回状态，返回本 tick 是否处于公平模式。
//
// enter = 0：不做判定，永远公平模式（= 改造前行为，也是「不配就不启用滞回」）。
// 越过 enter 进入；回落到 exit 以下并连续 exitTicks 个 tick 才退出 ——
// 避免在 89%/91%/89% 之间反复抖动（FR-076）。
func (s *NodeFairScheduler) updateCongestion(used, root uint64) bool {
	enter := uint64(s.congestionEnterPercent.Load())
	if enter == 0 {
		s.congested = true
		s.belowExitTicks = 0
		return true
	}
	exit := uint64(s.congestionExitPercent.Load())
	if exit == 0 || exit > enter {
		exit = enter
	}
	exitTicks := int(s.congestionExitTicks.Load())
	if exitTicks == 0 {
		exitTicks = 1
	}
	util := used * 100 / root

	if !s.congested {
		if util >= enter {
			s.congested = true
			s.belowExitTicks = 0
		}
		return s.congested
	}
	if util <= exit {
		s.belowExitTicks++
		if s.belowExitTicks >= exitTicks {
			s.congested = false
			s.belowExitTicks = 0
		}
	} else {
		s.belowExitTicks = 0
	}
	return s.congested
}

// ceilingFor 是这个成员任何时刻都不该超过的速率：
// min(root_cap, 他买的天花板, 他 class 当前允许的峰值)。
func (s *NodeFairScheduler) ceilingFor(m *fairMember, root uint64) uint64 {
	c := root
	if own := fairOwnLimitBytesPerSecond(m.user); own > 0 && own < c {
		c = own
	}
	if p := s.ClassPolicyFor(m.className()); p != nil {
		if cls := m.credit.ceilingBytePerSec(p); cls > 0 && cls < c {
			c = cls
		}
	}
	return c
}

// fill 是注水法本体：加权 max-min 公平，结果写进每个成员的 alloc。
//
// 约束态（有人被权重份额压住）时 Σ alloc ≤ root；非约束态（大家加起来都没要满）
// 直接把每人放到自己的天花板 —— work-conserving 的定义，不是超发。
func (s *NodeFairScheduler) fill(active []*fairMember, root uint64) {
	for _, m := range active {
		m.ceiling = s.ceilingFor(m, root)
		m.weight = 1
		if p := s.ClassPolicyFor(m.className()); p != nil && p.Weight > 0 {
			m.weight = uint64(p.Weight)
		}
	}

	s.assignFloors(active, root)

	pool := root
	for _, m := range active {
		m.alloc = m.floor
		m.pinned = false
		pool -= m.floor // assignFloors 已保证 Σ floor ≤ root

		switch {
		case m.backlogged:
			// 阻塞过 = 还想要更多，具体多少不必猜：拿他的天花板当需求上界。
			m.want = m.ceiling
		default:
			// satisfied：需求就是实测吞吐，加 1/8 抬头防每秒抖动。
			m.want = m.lastDelta + m.lastDelta/fairSatisfiedHeadroomDiv
			if m.want > m.ceiling {
				m.want = m.ceiling
			}
		}
		if m.want < m.floor {
			m.want = m.floor
		}
		if m.ceiling <= m.floor {
			m.alloc = m.floor
			m.pinned = true
		}
	}

	constrained := false
	for round := 0; ; round++ {
		var totalWeight uint64
		for _, m := range active {
			if !m.pinned {
				totalWeight += m.weight
			}
		}
		if totalWeight == 0 {
			break
		}
		if round >= fairFillMaxRounds {
			splitRemainder(active, pool, totalWeight)
			constrained = true
			break
		}
		progressed := false
		for _, m := range active {
			if m.pinned {
				continue
			}
			if share := m.floor + pool*m.weight/totalWeight; m.want <= share {
				m.alloc = m.want
				pool -= m.want - m.floor
				m.pinned = true
				progressed = true
			}
		}
		if !progressed {
			// 无人新饱和 → 剩下的按权重分完，这一步就是「被压住」。
			splitRemainder(active, pool, totalWeight)
			constrained = true
			break
		}
	}

	if !constrained {
		// 谁也没被压住 —— 节点其实不挤，别拿上一 tick 的实测吞吐把人钉死，
		// 否则一个刚开始下载的人要好几秒才能爬上来。
		for _, m := range active {
			m.alloc = m.ceiling
		}
	}
}

func splitRemainder(active []*fairMember, pool, totalWeight uint64) {
	for _, m := range active {
		if m.pinned {
			continue
		}
		m.alloc = m.floor + pool*m.weight/totalWeight
		m.pinned = true
	}
}

// assignFloors 给每个活跃成员定地板，并保证 Σ floor ≤ root。
//
// 这是与旧行为最重要的区别（FR-077）。旧代码在 share < hard 时无条件把每个人抬到
// hard，不管 hard×N 是否给得起：160,000 B/s 的节点、50 个活跃用户，全员抬到 16,384
// → 合计 819,200，是 root_cap 的 5.1 倍。发出去的额度比水管还大，队列就排到上游
// 运营商的缓冲区里去了，那里我们既看不见也控制不了。
//
// 现在按三档往下退：
//
//	A  max(class 地板, 软地板)  给得起就用
//	B  硬地板                  给得起就用
//	C  不给                    拥挤到人均 0.1 Mbps 时地板没有意义，那就是纯公平竞争
//
// 地板还要被自己的天花板夹住：给一个只买了 8KB/s 的人 16KB/s 的地板毫无意义，
// 他也跑不掉，白白吃掉别人的份额。
func (s *NodeFairScheduler) assignFloors(active []*fairMember, root uint64) {
	soft := s.softFloorBytePerSec.Load()
	hard := s.hardFloorBytePerSec.Load()
	if soft > 0 && hard > soft {
		hard = soft // 倒挂夹平
	}

	var sum uint64
	for _, m := range active {
		f := soft
		if p := s.ClassPolicyFor(m.className()); p != nil && p.FloorRatioPercent > 0 {
			if cf := p.NormalCapBytePerSec * uint64(p.FloorRatioPercent) / 100; cf > f {
				f = cf
			}
		}
		if f > m.ceiling {
			f = m.ceiling
		}
		m.floor = f
		sum += f
	}
	// sum == 0 表示这一档根本没配（不是「配了但正好为零」），要继续往下试硬地板，
	// 否则「只配硬地板不配软地板」这个最常见的配法会一路静默地退化成无地板。
	if sum > 0 && sum <= root {
		return
	}

	sum = 0
	for _, m := range active {
		f := hard
		if f > m.ceiling {
			f = m.ceiling
		}
		m.floor = f
		sum += f
	}
	if sum <= root {
		return
	}

	for _, m := range active {
		m.floor = 0
	}
}

// burstFor 由速率推 burst。
//   - 普通成员：1/8 秒配额（125ms 窗口）。整秒 burst 会造成 1s 突发 + 1s 静默的锯齿。
//   - 有突发策略的成员：25ms 窗口（FR-078）。burst_cap 常常是基准的 5~6 倍，
//     用 125ms 窗口会让它一次性倾泻近 2MB，整形就成了摆设。
//
// floor 到单次读缓冲，防小速率下过碎。
func (s *NodeFairScheduler) burstFor(m *fairMember, bps uint64) int {
	div := uint64(8)
	if p := s.ClassPolicyFor(m.className()); p != nil && p.BurstCreditBytes > 0 && p.BurstCapBytePerSec > p.NormalCapBytePerSec {
		div = 1000 / fairBurstShapingWindowMsec
	}
	b := int(bps / div)
	if b < fairBurstFloorB {
		b = fairBurstFloorB
	}
	return b
}

// setLimit 同步更新速率与 burst。wrapper 侧每轮 WaitN 动态读 Burst() 并对并发缩小
// 重试，因此这里可以安全 SetBurst。
func (s *NodeFairScheduler) setLimit(m *fairMember, bps uint64) {
	lim := rate.Limit(bps)
	burst := s.burstFor(m, bps)
	m.upLimiter.SetLimit(lim)
	m.upLimiter.SetBurst(burst)
	m.downLimiter.SetLimit(lim)
	m.downLimiter.SetBurst(burst)
}
