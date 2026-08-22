package protocol

import "testing"

// newSched 造一个隔离的调度器（不碰进程单例），便于确定性测 recompute。
//
// 注意默认没有拥塞阈值（congestionEnterPercent = 0），也就是「永远公平模式」——
// 大多数用例想测的是分配算法本身，不是拥塞判定。测拥塞判定的用例自己设阈值。
func newSched(rootCapBytePerSec uint64) *NodeFairScheduler {
	s := &NodeFairScheduler{members: make(map[string]*fairMember)}
	s.rootCapBytePerSec.Store(rootCapBytePerSec)
	s.started.Store(true) // 抑制后台 goroutine，测试手动调 recompute
	return s
}

func memberFor(s *NodeFairScheduler, email string, ownBitPerSec uint64) *fairMember {
	u := &MemoryUser{Email: email, BandwidthBps: ownBitPerSec}
	if up, _ := s.Member(u); up == nil {
		return nil // root_cap 为 0 时 Member 返回 nil
	}
	return s.members[email]
}

func memberIn(s *NodeFairScheduler, email, class string, ownBitPerSec uint64) *fairMember {
	u := &MemoryUser{Email: email, Class: class, BandwidthBps: ownBitPerSec}
	if up, _ := s.Member(u); up == nil {
		return nil
	}
	return s.members[email]
}

// satisfied 模拟一个 tick：跑了 n 字节，一次都没被令牌桶拦下。
func satisfied(m *fairMember, n uint64) { m.bytes.Add(n) }

// backlogged 模拟一个 tick：跑了 n 字节，且撞过桶——他还想要更多。
func backlogged(m *fairMember, n uint64) {
	m.bytes.Add(n)
	m.blocked.Add(1)
}

func upLimit(m *fairMember) uint64 { return uint64(m.upLimiter.Limit()) }

// ---- 基本分配 ----

// 单个 backlogged 用户：没人跟他抢，拿到自己的天花板。
func TestSingleActiveCappedByOwn(t *testing.T) {
	s := newSched(100_000_000)           // root_cap 100MB/s
	m := memberFor(s, "u1", 160_000_000) // own 160Mbit/s = 20MB/s
	if m == nil {
		t.Fatal("member nil (root_cap 没设？)")
	}
	backlogged(m, 1<<20)
	s.recompute()
	if got := upLimit(m); got != 20_000_000 {
		t.Errorf("up: want 20000000, got %d", got)
	}
	if got := uint64(m.downLimiter.Limit()); got != 20_000_000 {
		t.Errorf("down: want 20000000, got %d", got)
	}
}

// 全员 backlogged 且天花板都高于份额：等权重均分（这是均分仍然正确的那种情形）。
func TestBackloggedEqualWeightSplit(t *testing.T) {
	s := newSched(100_000_000)
	for _, e := range []string{"a", "b", "c", "d"} {
		backlogged(memberFor(s, e, 400_000_000), 1<<20)
	}
	s.recompute()
	for _, e := range []string{"a", "b", "c", "d"} {
		if got := upLimit(s.members[e]); got != 25_000_000 {
			t.Errorf("%s: want 25000000, got %d", e, got)
		}
	}
}

// unlimited 用户（own=0）也纳入公平，不绕过节点上限。
func TestUnlimitedUserCappedByShare(t *testing.T) {
	s := newSched(60_000_000)
	backlogged(memberFor(s, "a", 0), 1<<20)
	backlogged(memberFor(s, "b", 0), 1<<20)
	s.recompute()
	if got := upLimit(s.members["a"]); got != 30_000_000 {
		t.Errorf("unlimited a: want 30000000, got %d", got)
	}
}

// 非活跃用户还原个人上限，不占份额分母。
func TestIdleRestoresOwn(t *testing.T) {
	s := newSched(100_000_000)
	act := memberFor(s, "act", 640_000_000) // 80MB/s
	memberFor(s, "idle", 80_000_000)        // 10MB/s
	backlogged(act, 1<<20)
	s.recompute()
	if got := upLimit(act); got != 80_000_000 {
		t.Errorf("act: want 80000000, got %d", got)
	}
	if got := upLimit(s.members["idle"]); got != 10_000_000 {
		t.Errorf("idle: want own 10000000, got %d", got)
	}
}

// 节点公平未开启（root_cap=0）时 Member 返回 nil，调用方不挂 wrapper。
func TestMemberNilWhenDisabled(t *testing.T) {
	s := newSched(0)
	if up, down := s.Member(&MemoryUser{Email: "x", BandwidthBps: 1000}); up != nil || down != nil {
		t.Error("root_cap=0 时必须返回 nil limiter")
	}
}

// ---- 本次改造的核心：需求感知，剩余带宽流向真正想要的人 ----

// 完成定义 #2。480 Mbps 节点、500 个客户，其中 300 个只要 0.17 Mbps。
//
// 旧的人头均分给每个人 avail/500 = 120,000 B/s（0.96 Mbps），那 300 个人根本用不掉，
// 另外 200 个人也只能拿 0.96 Mbps。新调度器按实测需求扣掉低需求者，剩下的全给
// backlogged 的人。
func TestLowDemandDoesNotHoardShare(t *testing.T) {
	const (
		rootCap   = 60_000_000 // 480 Mbps
		lowDemand = 21_250     // 0.17 Mbps
		lowCount  = 300
		highCount = 200
	)
	s := newSched(rootCap)

	low := make([]*fairMember, 0, lowCount)
	high := make([]*fairMember, 0, highCount)
	for i := 0; i < lowCount; i++ {
		m := memberFor(s, "low"+itoa(i), 0)
		satisfied(m, lowDemand)
		low = append(low, m)
	}
	for i := 0; i < highCount; i++ {
		m := memberFor(s, "high"+itoa(i), 0)
		backlogged(m, 1<<20)
		high = append(high, m)
	}
	s.recompute()

	headCount := uint64(rootCap) / (lowCount + highCount) // 对照组：改造前的人头均分
	got := upLimit(high[0])
	if got <= headCount {
		t.Fatalf("高需求者只拿到 %d B/s，不比人头均分 %d B/s 多，剩余带宽没流过去", got, headCount)
	}
	// 低需求者按实测需求（+1/8 抬头）拿，不再占着一整份。
	if l := upLimit(low[0]); l > lowDemand*2 {
		t.Errorf("低需求者拿了 %d B/s，远超他实际要的 %d B/s", l, lowDemand)
	}
	// 总额守得住。
	var total uint64
	for _, m := range s.members {
		total += m.alloc
	}
	if total > rootCap {
		t.Errorf("Σ allocation = %d > root_cap %d", total, rootCap)
	}
	t.Logf("人头均分 %d B/s (%.2f Mbps) → 需求感知 %d B/s (%.2f Mbps)，提升 %.2f 倍；Σ=%d ≤ root_cap=%d",
		headCount, float64(headCount)*8/1e6, got, float64(got)*8/1e6, float64(got)/float64(headCount), total, rootCap)
}

// FR-075 空闲额度立刻回流：低需求者停下来，当 tick 高需求者就涨速。
func TestIdleCapacityFlowsBackSameTick(t *testing.T) {
	s := newSched(10_000_000)
	hog := memberFor(s, "hog", 0)
	quiet := memberFor(s, "quiet", 0)

	backlogged(hog, 1<<20)
	backlogged(quiet, 1<<20)
	s.recompute()
	before := upLimit(hog)
	if before != 5_000_000 {
		t.Fatalf("两人争抢: want 5000000, got %d", before)
	}

	// quiet 停了（不再有字节、不再阻塞），hog 继续撞桶。
	backlogged(hog, 1<<20)
	s.recompute()
	if got := upLimit(hog); got <= before {
		t.Errorf("对方停下后 hog 应立刻涨速：before %d, got %d", before, got)
	}
}

// ---- 地板：给得起才给（FR-077，与旧实现最重要的区别）----

// 完成定义 #4。极端拥挤（人均低于硬地板）时，发出去的额度合计不超过 root_cap。
//
// 旧契约是「只慢不断连」：无条件把每个人抬到硬地板，不管给不给得起。按这里的参数
// （root_cap 160,000 B/s、50 个活跃用户、硬地板 16,384）旧实现会发出 819,200 B/s，
// 是 root_cap 的 5.1 倍——节点级公平在最需要它的时刻完全失效，真实排队跑到上游
// 运营商的缓冲区里去，那里我们既看不见也控制不了（违反 FR-072）。
//
// 主人 2026-08-22 裁定换成「节点总出口守得住」：拥挤到人均 0.1 Mbps 时地板没有意义，
// 那时候就是纯公平竞争。本测试就是新契约。
func TestExtremeCongestionNeverOverAllocatesRootCap(t *testing.T) {
	const (
		rootCap   = 160_000
		users     = 50
		hardFloor = 16_384
	)
	s := newSched(rootCap)
	s.SetFloors(0, hardFloor)
	for i := 0; i < users; i++ {
		backlogged(memberFor(s, "u"+itoa(i), 0), 1<<20)
	}
	s.recompute()

	var total uint64
	for _, m := range s.members {
		total += m.alloc
		if got := upLimit(m); got != rootCap/users {
			t.Errorf("%s: 地板给不起就该纯均分 %d，got %d", m.user.Email, rootCap/users, got)
		}
	}
	if total > rootCap {
		t.Fatalf("Σ allocation = %d > root_cap %d（旧实现在这里是 %d，5.1 倍）",
			total, rootCap, users*hardFloor)
	}
	t.Logf("50 人挤 %d B/s：Σ allocation = %d（旧实现 %d = root_cap 的 %.1f 倍）",
		rootCap, total, users*hardFloor, float64(users*hardFloor)/rootCap)
}

// 给得起的时候地板照样生效——不是把地板整个删掉。
func TestFloorAppliesWhenAffordable(t *testing.T) {
	s := newSched(1_000_000)
	s.SetFloors(0, 16_384)
	// 5 个人，硬地板合计 81,920 ≤ 1,000,000，给得起。
	// 其中 4 个人是低需求，第 5 个人一个字节都没跑但仍活跃 → 靠地板兜住。
	for i := 0; i < 4; i++ {
		backlogged(memberFor(s, "hog"+itoa(i), 0), 1<<20)
	}
	tiny := memberFor(s, "tiny", 0)
	satisfied(tiny, 5000) // 刚过活跃阈值 4096，实测需求只有 5000
	s.recompute()
	if got := upLimit(tiny); got != 16_384 {
		t.Errorf("地板给得起时应兜到 16384，got %d", got)
	}
}

// 地板要被自己的天花板夹住：给一个只买了 8KB/s 的人 16KB/s 的地板毫无意义，
// 他也跑不掉，白白吃掉别人的份额。旧实现在这里会把公平桶抬到 16384。
func TestFloorClampedByOwnCeiling(t *testing.T) {
	s := newSched(10_000_000)
	s.SetFloors(0, 16_384)
	m := memberFor(s, "tiny", 64_000) // own 64Kbit/s = 8000 B/s
	backlogged(m, 1<<20)
	s.recompute()
	if got := upLimit(m); got != 8_000 {
		t.Errorf("want own ceiling 8000, got %d", got)
	}
}

// FR-079c：0 = 无地板，不是「用默认值」。一个魔数都不许藏在代码里。
func TestZeroFloorsMeanNoFloor(t *testing.T) {
	const rootCap = 12_500_000 // 100 Mbps
	const users = 1000
	s := newSched(rootCap)
	s.SetFloors(0, 0) // 明确不配
	for i := 0; i < users; i++ {
		backlogged(memberFor(s, "u"+itoa(i), 0), 1<<20)
	}
	s.recompute()
	var total uint64
	for _, m := range s.members {
		total += m.alloc
	}
	if total != rootCap {
		t.Errorf("Σ allocation = %d，want 正好 %d（旧实现会用默认硬地板 16384 发出 %d）",
			total, rootCap, users*16384)
	}
	if got := upLimit(s.members["u0"]); got != rootCap/users {
		t.Errorf("want %d, got %d", rootCap/users, got)
	}
}

// 只配硬地板不配软地板（最常见的配法）也要生效，不能静默退化成无地板。
func TestHardFloorAloneStillApplies(t *testing.T) {
	s := newSched(1_000_000)
	s.SetFloors(0, 20_000)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)
	backlogged(a, 1<<20)
	satisfied(b, 5000)
	s.recompute()
	if got := upLimit(b); got != 20_000 {
		t.Errorf("硬地板单独配也要生效: want 20000, got %d", got)
	}
}

// 软地板给得起时优先用软地板；给不起就退到硬地板；再给不起就不给。
func TestFloorTiersDegrade(t *testing.T) {
	const soft, hard = 62_500, 16_384
	cases := []struct {
		name    string
		rootCap uint64
		users   int
		want    uint64
	}{
		{"软地板给得起", 1_000_000, 10, soft},
		{"软给不起硬给得起", 200_000, 10, hard},
		{"都给不起就不给", 100_000, 50, 100_000 / 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newSched(c.rootCap)
			s.SetFloors(soft, hard)
			var quiet *fairMember
			for i := 0; i < c.users; i++ {
				m := memberFor(s, "u"+itoa(i), 0)
				if i == 0 {
					quiet = m
					satisfied(m, 5000) // 低需求，只能靠地板兜
				} else {
					backlogged(m, 1<<20)
				}
			}
			s.recompute()
			if got := upLimit(quiet); got != c.want {
				t.Errorf("want %d, got %d", c.want, got)
			}
			var total uint64
			for _, m := range s.members {
				total += m.alloc
			}
			if total > c.rootCap {
				t.Errorf("Σ allocation = %d > root_cap %d", total, c.rootCap)
			}
		})
	}
}

// 倒挂配置（hard > soft）夹平，避免「硬地板比软地板还高」这种自相矛盾的配法。
func TestInvertedFloorsClamped(t *testing.T) {
	s := newSched(1_000_000)
	s.SetFloors(10_000, 50_000)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)
	backlogged(a, 1<<20)
	satisfied(b, 5000)
	s.recompute()
	// 软地板 10,000 给得起 → 直接用软地板，硬地板不参与。
	if got := upLimit(b); got != 10_000 {
		t.Errorf("want 10000, got %d", got)
	}
}

// ---- 拥塞滞回（FR-076）----

// 不挤的时候根本不削速；越过上阈值才进公平模式；回落并持续数 tick 才退出。
func TestCongestionHysteresis(t *testing.T) {
	const rootCap = 1_000_000
	s := newSched(rootCap)
	s.SetCongestionHysteresis(80, 60, 3)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)

	// 利用率 60% < 80%：不削速，每人拿自己的天花板（=root_cap）。
	backlogged(a, 300_000)
	backlogged(b, 300_000)
	s.recompute()
	if got := upLimit(a); got != rootCap {
		t.Fatalf("60%% 利用率不该削速: want %d, got %d", rootCap, got)
	}

	// 利用率 90% ≥ 80%：进公平模式。
	backlogged(a, 450_000)
	backlogged(b, 450_000)
	s.recompute()
	if got := upLimit(a); got != rootCap/2 {
		t.Fatalf("越过上阈值应进公平模式: want %d, got %d", rootCap/2, got)
	}

	// 利用率 70%：在 60~80 之间，滞回带内，保持公平模式（不抖）。
	backlogged(a, 350_000)
	backlogged(b, 350_000)
	s.recompute()
	if got := upLimit(a); got != rootCap/2 {
		t.Fatalf("滞回带内应保持公平模式: want %d, got %d", rootCap/2, got)
	}

	// 利用率 40% ≤ 60%：开始计数，但要连续 3 tick 才退出。
	for i := 0; i < 2; i++ {
		backlogged(a, 200_000)
		backlogged(b, 200_000)
		s.recompute()
		if got := upLimit(a); got != rootCap/2 {
			t.Fatalf("第 %d 个低 tick 不该立刻退出: got %d", i+1, got)
		}
	}
	backlogged(a, 200_000)
	backlogged(b, 200_000)
	s.recompute()
	if got := upLimit(a); got != rootCap {
		t.Errorf("连续 3 tick 低于下阈值应退出公平模式: want %d, got %d", rootCap, got)
	}
}

// enter=0 = 不做拥塞判定（不配就不启用），永远公平模式。
func TestCongestionDisabledMeansAlwaysFair(t *testing.T) {
	s := newSched(1_000_000)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)
	backlogged(a, 1000)
	backlogged(b, 1000) // 利用率 0.2%，但没配阈值
	// 字节太少不算活跃，补到活跃阈值以上
	backlogged(a, 1<<20)
	backlogged(b, 1<<20)
	s.recompute()
	if got := upLimit(a); got != 500_000 {
		t.Errorf("不配阈值 = 永远公平模式: want 500000, got %d", got)
	}
}

// ---- class 权重（完成定义 #3）----

func liveVsShort(s *NodeFairScheduler) {
	s.SetClassPolicies([]*ClassPolicy{
		{Name: "live", Weight: 4, NormalCapBytePerSec: 3_000_000},
		{Name: "short", Weight: 1, NormalCapBytePerSec: 3_000_000},
	})
}

// 高负载：直播按权重被优先保证，短视频被压但没被饿死。
func TestClassWeightPrioritisesLiveWithoutStarvingShort(t *testing.T) {
	s := newSched(6_000_000)
	liveVsShort(s)
	var live, short []*fairMember
	for i := 0; i < 2; i++ {
		m := memberIn(s, "live"+itoa(i), "live", 0)
		backlogged(m, 1<<20)
		live = append(live, m)
		n := memberIn(s, "short"+itoa(i), "short", 0)
		backlogged(n, 1<<20)
		short = append(short, n)
	}
	s.recompute()

	l, sh := upLimit(live[0]), upLimit(short[0])
	if l != 4*sh {
		t.Errorf("直播:短视频 应为 weight 比 4:1，got %d:%d", l, sh)
	}
	if sh == 0 {
		t.Fatal("短视频被饿死了")
	}
	var total uint64
	for _, m := range s.members {
		total += m.alloc
	}
	if total > 6_000_000 {
		t.Errorf("Σ allocation = %d > root_cap 6000000", total)
	}
	t.Logf("拥塞时 直播 %d B/s (%.1f Mbps) / 短视频 %d B/s (%.1f Mbps)，Σ=%d",
		l, float64(l)*8/1e6, sh, float64(sh)*8/1e6, total)
}

// 直播空闲时，短视频吃满自己的 normal_cap——不许因为「直播权重高」就给它留着。
func TestShortVideoFillsNormalCapWhenLiveIdle(t *testing.T) {
	s := newSched(6_000_000)
	liveVsShort(s)
	memberIn(s, "live0", "live", 0) // 建了成员但一个字节都不跑
	memberIn(s, "live1", "live", 0)
	var short []*fairMember
	for i := 0; i < 2; i++ {
		m := memberIn(s, "short"+itoa(i), "short", 0)
		backlogged(m, 1<<20)
		short = append(short, m)
	}
	s.recompute()
	if got := upLimit(short[0]); got != 3_000_000 {
		t.Errorf("直播空闲时短视频应吃满 normal_cap 3000000, got %d", got)
	}
}

// class 的 normal_cap 是「不挤时的上限」，不是保证带宽（FR-071）：
// 挤的时候拿不到 normal_cap 是正常的，不是 bug。
func TestNormalCapIsNotAGuarantee(t *testing.T) {
	s := newSched(6_000_000)
	liveVsShort(s)
	for i := 0; i < 10; i++ {
		backlogged(memberIn(s, "short"+itoa(i), "short", 0), 1<<20)
	}
	s.recompute()
	got := upLimit(s.members["short0"])
	if got >= 3_000_000 {
		t.Fatalf("10 个人挤 6MB/s 还能人人拿到 normal_cap？那就是把它实现成 CIR 了：got %d", got)
	}
	if got != 600_000 {
		t.Errorf("want 600000, got %d", got)
	}
}

// 未分类用户落到名字为空的兜底策略；没有兜底策略就是同权重无上限。
func TestClassFallback(t *testing.T) {
	s := newSched(1_000_000)
	if p := s.ClassPolicyFor("unknown"); p != nil {
		t.Fatal("没有策略表时应返回 nil")
	}
	s.SetClassPolicies([]*ClassPolicy{
		{Name: "", Weight: 1, NormalCapBytePerSec: 100_000},
		{Name: "live", Weight: 4},
	})
	if p := s.ClassPolicyFor("unknown"); p == nil || p.NormalCapBytePerSec != 100_000 {
		t.Error("未知 class 应落到兜底策略")
	}
	if p := s.ClassPolicyFor("live"); p == nil || p.Weight != 4 {
		t.Error("精确匹配优先")
	}
}

// class 地板：floor_ratio 给的地板与全局软地板取较大者，仍受「给得起才给」约束。
func TestClassFloorRatio(t *testing.T) {
	s := newSched(6_000_000)
	s.SetClassPolicies([]*ClassPolicy{
		{Name: "live", Weight: 4, NormalCapBytePerSec: 2_000_000, FloorRatioPercent: 50},
		{Name: "short", Weight: 1, NormalCapBytePerSec: 2_000_000},
	})
	quiet := memberIn(s, "live0", "live", 0)
	satisfied(quiet, 5000) // 直播用户只跑了一点点
	for i := 0; i < 4; i++ {
		backlogged(memberIn(s, "short"+itoa(i), "short", 0), 1<<20)
	}
	s.recompute()
	if got := upLimit(quiet); got != 1_000_000 {
		t.Errorf("class 地板 = normal_cap 2000000 × 50%% = 1000000, got %d", got)
	}
}

// ---- 活跃滞回与清理（保留行为）----

// 间歇流量用户不在 share↔own 之间每秒跳变。
func TestActiveHysteresis(t *testing.T) {
	s := newSched(8_000_000)
	a := memberFor(s, "steady", 0)
	b := memberFor(s, "bursty", 0)

	tick := func(aDelta, bDelta uint64) {
		backlogged(a, aDelta)
		if bDelta > 0 {
			backlogged(b, bDelta)
		}
		s.recompute()
	}

	tick(1<<20, 1<<20) // 双活跃 → 各 4MB
	if got := upLimit(b); got != 4_000_000 {
		t.Fatalf("tick1: want 4000000, got %d", got)
	}
	// b 静默 1、2 tick：滞回保持活跃（旧行为会立刻弹回天花板）
	tick(1<<20, 0)
	tick(1<<20, 0)
	if !b.active {
		t.Error("静默 2 tick 不该退出活跃")
	}
	// 中间带流量（2KB ∈ [1KB,4KB)）：保持活跃并清零退出计数
	backlogged(a, 1<<20)
	b.bytes.Add(2 * 1024)
	s.recompute()
	if !b.active {
		t.Error("中间带流量应保持活跃")
	}
	// 连续 3 tick < 1KB → 退出活跃 → 还原到天花板（own=0 → root_cap）
	tick(1<<20, 0)
	tick(1<<20, 0)
	tick(1<<20, 0)
	if b.active {
		t.Error("连续 3 tick 低于退出阈值应退出活跃")
	}
	if got := upLimit(b); got != 8_000_000 {
		t.Errorf("退出活跃后应还原天花板: want 8000000, got %d", got)
	}
}

// 惰性清理：连续 fairMemberExpireTicks 轮零字节且零连接的成员被移除；有连接的保留。
func TestMemberLazyCleanup(t *testing.T) {
	s := newSched(1_000_000)
	memberFor(s, "gone", 0)
	held := memberFor(s, "held", 0)
	held.conns.Add(1)
	for i := 0; i < fairMemberExpireTicks; i++ {
		s.recompute()
	}
	if _, ok := s.members["gone"]; ok {
		t.Error("零流量零连接的成员应被清理")
	}
	if _, ok := s.members["held"]; !ok {
		t.Error("有活连接的成员必须保留")
	}

	s2 := newSched(1_000_000)
	m := memberFor(s2, "traffic", 0)
	for i := 0; i < fairMemberExpireTicks; i++ {
		if i == fairMemberExpireTicks/2 {
			satisfied(m, 100)
		}
		s2.recompute()
	}
	if _, ok := s2.members["traffic"]; !ok {
		t.Error("中途有流量的成员不该被清理")
	}
}

// ---- 天花板取值（双速率）----

// fairOwnLimitBytesPerSecond 的真值表：PIR 有就用 PIR，没有退到 CIR，都没有才是 0。
func TestFairOwnLimitFallsBackToCommitted(t *testing.T) {
	cases := []struct {
		name string
		pir  uint64
		cir  uint64
		want uint64
	}{
		{"两个都没有 = 无天花板", 0, 0, 0},
		{"只有 PIR", 80_000_000, 0, 10_000_000},
		{"只有 CIR：天花板就是 CIR", 0, 8_000_000, 1_000_000},
		{"双速率：天花板是 PIR", 80_000_000, 8_000_000, 10_000_000},
		{"CIR 高于 PIR：仍以 PIR 为天花板", 8_000_000, 80_000_000, 1_000_000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := &MemoryUser{Email: "x", BandwidthBps: c.pir, CommittedBps: c.cir}
			if got := fairOwnLimitBytesPerSecond(u); got != c.want {
				t.Errorf("want %d B/s, got %d", c.want, got)
			}
		})
	}
	if got := fairOwnLimitBytesPerSecond(nil); got != 0 {
		t.Errorf("nil user: want 0, got %d", got)
	}
}

func memberForDual(s *NodeFairScheduler, email string, pir, cir uint64) *fairMember {
	u := &MemoryUser{Email: email, BandwidthBps: pir, CommittedBps: cir}
	if up, _ := s.Member(u); up == nil {
		return nil
	}
	return s.members[email]
}

// 只配 CIR 的用户必须被自己的 CIR 封顶，否则他跑不满、节点容量空转。
func TestCommittedOnlyUserCappedByOwn(t *testing.T) {
	s := newSched(100_000_000)
	m := memberForDual(s, "cir-only", 0, 160_000_000) // CIR 160Mbit/s = 20MB/s
	backlogged(m, 1<<20)
	s.recompute()
	if got := upLimit(m); got != 20_000_000 {
		t.Errorf("want 20000000, got %d", got)
	}
}

// 非活跃分支也要还原到 CIR 天花板，而不是 root_cap。
func TestIdleCommittedOnlyRestoresOwn(t *testing.T) {
	s := newSched(100_000_000)
	act := memberForDual(s, "act", 640_000_000, 0)
	idle := memberForDual(s, "cir-idle", 0, 8_000_000)
	backlogged(act, 1<<20)
	s.recompute()
	if got := upLimit(idle); got != 1_000_000 {
		t.Errorf("want 1000000, got %d", got)
	}
}

// Member 首次建桶时的初始速率也走同一个天花板函数。
func TestMemberInitialRateUsesCommittedCeiling(t *testing.T) {
	s := newSched(100_000_000)
	m := memberForDual(s, "fresh", 0, 8_000_000)
	if got := upLimit(m); got != 1_000_000 {
		t.Errorf("want 1000000, got %d", got)
	}
}

// 双速率用户的天花板是 PIR：CIR 只在 CBS 花完后压低长期均值，不是天花板。
func TestDualRateUsesPeakAsCeiling(t *testing.T) {
	s := newSched(100_000_000)
	m := memberForDual(s, "dual", 160_000_000, 8_000_000)
	backlogged(m, 1<<20)
	s.recompute()
	if got := upLimit(m); got != 20_000_000 {
		t.Errorf("want 20000000, got %d", got)
	}
}

// ---- burst 窗口 ----

// 普通成员 burst = 1/8 秒配额，防「1 秒突发 + 1 秒静默」的锯齿；低速时落到单缓冲。
func TestSetLimitUpdatesBurst(t *testing.T) {
	s := newSched(1_000_000)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)
	backlogged(a, 1<<20)
	backlogged(b, 1<<20)
	s.recompute() // 各 500,000 → burst 62,500
	if got := a.upLimiter.Burst(); got != 62_500 {
		t.Errorf("burst: want 62500, got %d", got)
	}
	s.rootCapBytePerSec.Store(100_000)
	backlogged(a, 1<<20)
	backlogged(b, 1<<20)
	s.recompute() // 各 50,000 → 50,000/8 = 6,250 < 8KB → floor
	if got := a.upLimiter.Burst(); got != fairBurstFloorB {
		t.Errorf("burst floor: want %d, got %d", fairBurstFloorB, got)
	}
}

// 有突发策略的成员用 25ms 窗口：burst_cap 常是基准的 5~6 倍，
// 用 125ms 窗口会让它一次倾泻近 2MB，整形就成了摆设。
func TestBurstClassUsesShorterShapingWindow(t *testing.T) {
	s := newSched(100_000_000)
	s.SetClassPolicies([]*ClassPolicy{
		{Name: "burst", NormalCapBytePerSec: 2_500_000, BurstCapBytePerSec: 15_000_000, BurstCreditBytes: 1 << 30},
	})
	m := memberIn(s, "b0", "burst", 0)
	if got := m.upLimiter.Burst(); got != 15_000_000*fairBurstShapingWindowMsec/1000 {
		t.Errorf("25ms 窗口: want %d, got %d", 15_000_000*fairBurstShapingWindowMsec/1000, got)
	}
	plain := memberFor(s, "p0", 0)
	if got := plain.upLimiter.Burst(); got != 100_000_000/8 {
		t.Errorf("无突发策略的成员仍是 1/8 秒窗口: want %d, got %d", 100_000_000/8, got)
	}
}

// itoa 避免 import strconv 只为造测试用的 email。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
