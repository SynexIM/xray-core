package protocol

import "testing"

// newSched 造一个隔离的调度器（不碰进程单例），便于确定性测 recompute。
func newSched(availBps uint64) *NodeFairScheduler {
	s := &NodeFairScheduler{members: make(map[string]*fairMember)}
	s.availBps.Store(availBps)
	s.started.Store(true) // 抑制后台 recompute goroutine，测试手动调 recompute 确定性断言
	return s
}

func memberFor(s *NodeFairScheduler, email string, ownBps uint64) *fairMember {
	u := &MemoryUser{Email: email, BandwidthBps: ownBps}
	up, _ := s.Member(u)
	if up == nil {
		// availBps 为 0 时 Member 返回 nil；测试需先设 avail
		return nil
	}
	return s.members[email]
}

// addDelta 模拟一个 tick 内的字节增量（bytes 是累计值）。
func addDelta(m *fairMember, n uint64) { m.bytes.Add(n) }

// 单活跃用户：share=avail，受 own 上限封顶。
func TestRecomputeSingleActiveCappedByOwn(t *testing.T) {
	s := newSched(100_000_000)           // 100MB/s avail
	m := memberFor(s, "u1", 160_000_000) // 160Mbps = 20MB/s
	if m == nil {
		t.Fatal("member nil (avail not set?)")
	}
	m.bytes.Store(1 << 20) // 模拟本轮跑了 1MB（>阈值=活跃）
	s.recompute()
	// 活跃且 own(20MB) < share(100MB) → eff=20MB
	if got := s.members["u1"].upLimiter.Limit(); int(got) != 20_000_000 {
		t.Errorf("u1 up limit: want 20000000, got %d", int(got))
	}
	if got := s.members["u1"].downLimiter.Limit(); int(got) != 20_000_000 {
		t.Errorf("u1 down limit: want 20000000, got %d", int(got))
	}
}

// 多活跃均分：share=avail/active，own>share 时取 share。
func TestRecomputeFairShareSplit(t *testing.T) {
	s := newSched(100_000_000)
	for _, e := range []string{"a", "b", "c", "d"} {
		m := memberFor(s, e, 400_000_000) // 400Mbps = 50MB/s > share
		m.bytes.Store(1 << 20)
	}
	s.recompute()
	share := 100_000_000 / 4
	for _, e := range []string{"a", "b", "c", "d"} {
		if got := int(s.members[e].upLimiter.Limit()); got != share {
			t.Errorf("%s up limit: want %d, got %d", e, share, got)
		}
	}
}

// unlimited 用户（own=0）也纳入公平：eff=share，不绕过节点公平。
func TestRecomputeUnlimitedUserCappedByShare(t *testing.T) {
	s := newSched(60_000_000)
	a := memberFor(s, "a", 0) // unlimited
	b := memberFor(s, "b", 0) // unlimited
	a.bytes.Store(1 << 20)
	b.bytes.Store(1 << 20)
	s.recompute()
	share := 60_000_000 / 2
	if got := int(s.members["a"].upLimiter.Limit()); got != share {
		t.Errorf("unlimited a: want share %d, got %d", share, got)
	}
}

// 非活跃用户还原个人上限，不占份额分母。
func TestRecomputeIdleRestoresOwn(t *testing.T) {
	s := newSched(100_000_000)
	active := memberFor(s, "act", 640_000_000) // 80MB/s
	memberFor(s, "idle", 80_000_000)           // 10MB/s
	active.bytes.Store(1 << 20)                // 活跃
	// idle bytes 不动 → 非活跃
	s.recompute()
	// 仅 act 活跃 → share=avail=100MB；own=80MB<share → act=80MB
	if got := int(s.members["act"].upLimiter.Limit()); got != 80_000_000 {
		t.Errorf("act: want 80000000, got %d", got)
	}
	// idle 还原 own=10MB
	if got := int(s.members["idle"].upLimiter.Limit()); got != 10_000_000 {
		t.Errorf("idle: want own 10000000, got %d", got)
	}
}

// 节点公平未开启（avail=0）时 Member 返回 nil（调用方不挂 wrapper，零开销）。
func TestMemberNilWhenDisabled(t *testing.T) {
	s := newSched(0)
	up, down := s.Member(&MemoryUser{Email: "x", BandwidthBps: 1000})
	if up != nil || down != nil {
		t.Error("Member must return nil limiters when node fair disabled")
	}
}

// BandwidthBps is a wire/domain bit/s value. Node fairness runs xray's byte/s
// limiter, so own limits must be converted before creating or recomputing buckets.
func TestMemberConvertsOwnLimitBitsToRuntimeBytes(t *testing.T) {
	s := newSched(10_000_000)
	m := memberFor(s, "x", 8_000_000)
	if m == nil {
		t.Fatal("member nil (avail not set?)")
	}
	if got := int(m.upLimiter.Limit()); got != 1_000_000 {
		t.Fatalf("initial up limit: want 1000000 B/s, got %d", got)
	}

	m.bytes.Store(1 << 20)
	s.recompute()
	if got := int(m.downLimiter.Limit()); got != 1_000_000 {
		t.Fatalf("recomputed down limit: want 1000000 B/s, got %d", got)
	}
}

// ---- Bundle G 公平限速大修回归测试 ----

// 极端拥挤（soft×N > avail）：按物理份额均分，不再是旧的 1 字节/秒。
func TestFairShareCongestionPhysicalSplit(t *testing.T) {
	s := newSched(1_000_000) // 1MB/s
	for i := 0; i < 20; i++ {
		m := memberFor(s, string(rune('a'+i)), 0)
		addDelta(m, 1<<20)
	}
	s.recompute()
	// soft(62500)×20 = 1.25MB > 1MB → 物理均分 50_000（仍高于硬地板）
	for email, m := range s.members {
		if got := int(m.upLimiter.Limit()); got != 50_000 {
			t.Errorf("%s: want 50000, got %d", email, got)
		}
	}
}

// 绝对硬地板：活跃用户多到物理份额 < 16KB/s 时夹到 16KB/s，只慢不断连。
func TestFairShareHardFloor(t *testing.T) {
	s := newSched(160_000) // 160KB/s
	for i := 0; i < 50; i++ {
		m := memberFor(s, "u"+string(rune('0'+i%10))+string(rune('a'+i/10)), 0)
		addDelta(m, 1<<20)
	}
	s.recompute()
	for email, m := range s.members {
		if got := int(m.upLimiter.Limit()); got != fairHardFloorDefaultB {
			t.Errorf("%s: want hard floor %d, got %d", email, fairHardFloorDefaultB, got)
		}
	}
}

// 硬地板对 own-cap 也兜底：即使 min(own, share) 更小，公平桶不低于 hard。
func TestFairShareHardFloorOverOwnCap(t *testing.T) {
	s := newSched(10_000_000)
	m := memberFor(s, "tiny", 64_000) // own = 64Kbps = 8KB/s < hard 16KB/s
	addDelta(m, 1<<20)
	s.recompute()
	if got := int(m.upLimiter.Limit()); got != fairHardFloorDefaultB {
		t.Errorf("want hard floor %d (own 桶另行执行 8KB/s), got %d", fairHardFloorDefaultB, got)
	}
}

// 地板可经配置下发覆盖；0=默认；hard 不得高于 soft（倒挂夹平）。
func TestSetFloorsOverride(t *testing.T) {
	s := newSched(1_000_000)
	s.SetFloors(100_000, 50_000)
	if soft, hard := s.floors(); soft != 100_000 || hard != 50_000 {
		t.Errorf("floors: want (100000,50000), got (%d,%d)", soft, hard)
	}
	s.SetFloors(10_000, 50_000) // 倒挂
	if _, hard := s.floors(); hard != 10_000 {
		t.Errorf("inverted floors: hard want clamp to 10000, got %d", hard)
	}
	s.SetFloors(0, 0)
	if soft, hard := s.floors(); soft != fairSoftFloorDefaultB || hard != fairHardFloorDefaultB {
		t.Errorf("default floors: got (%d,%d)", soft, hard)
	}
}

// 活跃滞回：间歇流量用户不在 share↔own 之间每秒跳变；
// 退出需连续 3 tick 增量 < 1KB，中间带 [1KB,4KB) 保持活跃。
func TestActiveHysteresis(t *testing.T) {
	s := newSched(8_000_000)
	a := memberFor(s, "steady", 0)
	b := memberFor(s, "bursty", 0)

	tick := func(aDelta, bDelta uint64) {
		addDelta(a, aDelta)
		addDelta(b, bDelta)
		s.recompute()
	}
	limitB := func() int { return int(b.upLimiter.Limit()) }

	tick(1<<20, 8*1024) // 双活跃 → share = 4MB
	if got := limitB(); got != 4_000_000 {
		t.Fatalf("tick1: want share 4000000, got %d", got)
	}
	// b 静默 1、2 tick：滞回保持活跃（旧行为会立刻弹回 own/avail）
	tick(1<<20, 0)
	tick(1<<20, 0)
	if got := limitB(); got != 4_000_000 {
		t.Errorf("tick3 (idle 2 ticks): want still 4000000, got %d", got)
	}
	// 中间带流量（2KB ∈ [1KB,4KB)）：不重置为退出，也保持活跃并清零退出计数
	tick(1<<20, 2*1024)
	if got := limitB(); got != 4_000_000 {
		t.Errorf("tick4 (mid-band): want still 4000000, got %d", got)
	}
	// 连续 3 tick < 1KB → 退出活跃 → 还原（own=0 → avail=8MB）
	tick(1<<20, 0)
	tick(1<<20, 0)
	tick(1<<20, 0)
	if got := limitB(); got != 8_000_000 {
		t.Errorf("after 3 idle ticks: want restored 8000000, got %d", got)
	}
	if got := int(a.upLimiter.Limit()); got != 8_000_000 {
		t.Errorf("steady alone: want full avail 8000000, got %d", got)
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
		t.Error("idle member with zero conns should be expired")
	}
	if _, ok := s.members["held"]; !ok {
		t.Error("member with live conns must be retained")
	}
	// 过流量会重置清理计数
	s2 := newSched(1_000_000)
	m := memberFor(s2, "traffic", 0)
	for i := 0; i < fairMemberExpireTicks; i++ {
		if i == fairMemberExpireTicks/2 {
			addDelta(m, 100)
		}
		s2.recompute()
	}
	if _, ok := s2.members["traffic"]; !ok {
		t.Error("member with traffic mid-way must not be expired yet")
	}
}

// ---- 只配承诺速率（CIR）的用户也要有天花板 ----

// fairOwnLimitBytesPerSecond 的真值表：实际天花板 = PIR 有就用 PIR，没有退到 CIR，
// 都没有才是 0（= 无天花板）。
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
		// CIR > PIR 是配错，内核忽略 CIR 退化成单速率 PIR，天花板同样是 PIR。
		{"CIR 高于 PIR：仍以 PIR 为天花板", 8_000_000, 80_000_000, 1_000_000},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			u := &MemoryUser{Email: "x", BandwidthBps: c.pir, CommittedBps: c.cir}
			if got := fairOwnLimitBytesPerSecond(u); got != c.want {
				t.Errorf("own limit: want %d B/s, got %d", c.want, got)
			}
		})
	}
	if got := fairOwnLimitBytesPerSecond(nil); got != 0 {
		t.Errorf("nil user: want 0, got %d", got)
	}
}

// memberForDual 造一个带双速率字段的成员。
func memberForDual(s *NodeFairScheduler, email string, pirBps, cirBps uint64) *fairMember {
	u := &MemoryUser{Email: email, BandwidthBps: pirBps, CommittedBps: cirBps}
	if up, _ := s.Member(u); up == nil {
		return nil
	}
	return s.members[email]
}

// 回归守卫：只配 CIR 的用户在公平分配里必须被自己的 CIR 封顶，
// 不能因为 BandwidthBps==0 就被当成「无天花板」拿满份额——他跑不满，节点容量空转。
func TestRecomputeCommittedOnlyUserCappedByOwn(t *testing.T) {
	s := newSched(100_000_000)                        // 100MB/s avail，只有一个活跃用户 → share = 100MB/s
	m := memberForDual(s, "cir-only", 0, 160_000_000) // CIR 160Mbps = 20MB/s
	if m == nil {
		t.Fatal("member nil (avail not set?)")
	}
	m.bytes.Store(1 << 20) // 活跃
	s.recompute()
	if got := int(m.upLimiter.Limit()); got != 20_000_000 {
		t.Errorf("up limit: want own ceiling 20000000 B/s, got %d (只读 BandwidthBps 会得到 share=100000000)", got)
	}
	if got := int(m.downLimiter.Limit()); got != 20_000_000 {
		t.Errorf("down limit: want own ceiling 20000000 B/s, got %d", got)
	}
}

// 同一件事的另一半：非活跃分支（applyOwn）也要还原到 CIR 天花板，而不是 avail。
func TestRecomputeIdleCommittedOnlyRestoresOwn(t *testing.T) {
	s := newSched(100_000_000)
	active := memberForDual(s, "act", 640_000_000, 0)  // 80MB/s
	idle := memberForDual(s, "cir-idle", 0, 8_000_000) // CIR 8Mbps = 1MB/s
	active.bytes.Store(1 << 20)
	s.recompute()
	if got := int(idle.upLimiter.Limit()); got != 1_000_000 {
		t.Errorf("idle CIR-only: want restored to own ceiling 1000000 B/s, got %d", got)
	}
}

// Member 首次建桶时的初始速率也走同一个天花板函数：min(own, avail)。
func TestMemberInitialRateUsesCommittedCeiling(t *testing.T) {
	s := newSched(100_000_000)
	m := memberForDual(s, "fresh", 0, 8_000_000) // CIR 8Mbps = 1MB/s
	if m == nil {
		t.Fatal("member nil (avail not set?)")
	}
	if got := int(m.upLimiter.Limit()); got != 1_000_000 {
		t.Errorf("initial limit: want min(own 1000000, avail) = 1000000, got %d", got)
	}
}

// 双速率用户的天花板是 PIR：CIR 只在 CBS 花完后压低长期均值，不是天花板，
// 公平层不该拿 CIR 去掐他的峰值。
func TestRecomputeDualRateUsesPeakAsCeiling(t *testing.T) {
	s := newSched(100_000_000)
	m := memberForDual(s, "dual", 160_000_000, 8_000_000) // PIR 20MB/s, CIR 1MB/s
	m.bytes.Store(1 << 20)
	s.recompute()
	if got := int(m.upLimiter.Limit()); got != 20_000_000 {
		t.Errorf("dual rate: want PIR ceiling 20000000 B/s, got %d", got)
	}
}

// recompute 同步更新 burst = 1/8 秒配额（floor 到单缓冲），防低速时段积累整秒突发。
func TestSetLimitUpdatesBurst(t *testing.T) {
	s := newSched(1_000_000)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)
	addDelta(a, 1<<20)
	addDelta(b, 1<<20)
	s.recompute() // share = 500_000 → burst 62_500
	if got := a.upLimiter.Burst(); got != 62_500 {
		t.Errorf("burst: want 62500 (1/8s of 500000), got %d", got)
	}
	// 硬地板速率下 burst 落到 floor（8KB 单缓冲）
	s.availBps.Store(100_000)
	s.recompute() // share raw 50_000 < soft, ≥... 50_000*? soft×2=125000 > 100000 → 物理 50_000
	if got := a.upLimiter.Burst(); got != fairBurstFloorB {
		t.Errorf("burst floor: want %d, got %d", fairBurstFloorB, got)
	}
}
