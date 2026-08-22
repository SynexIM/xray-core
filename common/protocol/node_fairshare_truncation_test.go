package protocol

import "testing"

// 注水有一个轮数上限（fairFillMaxRounds）。撞上限时余下的池子按权重一次分完：
// 总额仍守得住，但那批成员之间的公平性是近似的。
//
// 这个文件守的不是「截断这件事对不对」，而是**截断能不能被看见**。
// 运维在现场看到的只是「分配有点不公平」；如果没有东西指向截断，
// 他会去查调度器逻辑，查半天查不出来。日志里有、没人看，等于没有；
// 每 tick 刷一行，也等于没有。所以是：日志只在进入/退出时各一行，
// 完整数字随时能从 Status() 读走。

// truncatingSched 造一个**必定撞轮数上限**的调度器。
//
// 等权重下注水两三轮就收敛，撞不上上限——这本身是好事，但也意味着要测截断
// 就得造出那个最坏形态：权重按 4 的幂递减，天花板取剩余池子的一半。
//
//	某一轮里，权重最高的那个成员分到池子的 3/4（≥ 1/2）→ 他被钉住；
//	下一位分到 3/16（< 下一轮他自己的天花板 1/4）→ 这一轮轮不到他。
//	他被钉住后池子减半、总权重掉到 1/4，比例关系原样重现 →
//	每轮不多不少钉住一个人。
//
// 权重比必须大于 2：比值为 2 时「他能被钉住」和「下一位不能」这两个条件
// 正好在 f=1/2 处相切，凑不出稳定的窗口。取 4 就有了宽裕的余量，
// 于是成员数超过轮数上限就必然截断，不靠凑参数。
func truncatingSched(t *testing.T, root uint64, n int) *NodeFairScheduler {
	t.Helper()
	if n <= fairFillMaxRounds {
		t.Fatalf("成员数 %d 不多于轮数上限 %d，撞不上截断", n, fairFillMaxRounds)
	}
	s := newSched(root)
	policies := make([]*ClassPolicy, 0, n)
	for i := 0; i < n; i++ {
		policies = append(policies, &ClassPolicy{Name: "w" + itoa(i), Weight: 1 << uint(2*(n-1-i))})
	}
	s.SetClassPolicies(policies)

	pool := root
	for i := 0; i < n; i++ {
		ceiling := pool / 2
		if ceiling == 0 {
			t.Fatalf("root_cap %d 撑不住 %d 个成员的阶梯，天花板算到 0 了", root, n)
		}
		// own limit 是比特/秒，天花板是字节/秒。
		m := memberIn(s, "u"+itoa(i), "w"+itoa(i), ceiling*8)
		backlogged(m, 1<<20)
		pool -= ceiling
	}
	return s
}

// 截断发生时，Status() 要能回答运维的三个问题：
// 这一 tick 截断了吗 · 还剩多少成员没轮到 · 持续多久了。
func TestFillTruncationIsVisibleInStatus(t *testing.T) {
	const (
		root = uint64(100_000_000)
		n    = fairFillMaxRounds + 4
	)
	s := truncatingSched(t, root, n)

	s.recompute()
	st := s.Status()
	if !st.FillTruncated {
		t.Fatalf("%d 个成员、轮数上限 %d，必定截断，但 Status 说没有", n, fairFillMaxRounds)
	}
	if st.FillRounds != fairFillMaxRounds {
		t.Errorf("撞顶时轮数应正好等于上限 %d，got %d", fairFillMaxRounds, st.FillRounds)
	}
	if st.FillUnresolved == 0 || st.FillUnresolved >= uint32(n) {
		t.Errorf("没轮到的成员数应在 (0,%d) 之间，got %d", n, st.FillUnresolved)
	}
	if st.ActiveMembers != uint32(n) {
		t.Errorf("活跃成员数是没轮到那个数的分母，必须一起报：want %d, got %d", n, st.ActiveMembers)
	}
	if st.FillTruncatedTicks != 1 {
		t.Errorf("第一次截断应报持续 1 tick，got %d", st.FillTruncatedTicks)
	}
	t.Logf("截断可见：%d/%d 个活跃成员没轮到，跑了 %d 轮（上限 %d），已持续 %d 秒，累计 %d 秒",
		st.FillUnresolved, st.ActiveMembers, st.FillRounds, fairFillMaxRounds,
		st.FillTruncatedTicks, st.FillTruncatedTotal)

	// 持续截断要累计，运维才知道「这是一直在发生」还是「刚才抖了一下」。
	for i := 0; i < 3; i++ {
		for _, m := range s.members {
			backlogged(m, 1<<20)
		}
		s.recompute()
	}
	if got := s.Status().FillTruncatedTicks; got != 4 {
		t.Errorf("连续截断 4 tick，want 4, got %d", got)
	}
	if got := s.Status().FillTruncatedTotal; got != 4 {
		t.Errorf("累计截断 want 4, got %d", got)
	}
}

// 截断结束要归零，否则运维看到的永远是「还在截断」——那比没有指标更糟。
// 累计数不清零：事后复盘要靠它。
func TestTruncationClearsWhenItStops(t *testing.T) {
	const root = uint64(100_000_000)
	s := truncatingSched(t, root, fairFillMaxRounds+4)
	s.recompute()
	if !s.Status().FillTruncated {
		t.Fatal("前提不成立：应该先截断")
	}

	// 让成员全部退出活跃（不再有字节），下一 tick 根本不跑注水。
	for i := 0; i < fairActiveExitTicks; i++ {
		s.recompute()
	}
	st := s.Status()
	if st.FillTruncated {
		t.Error("不再截断了，标志位必须落下来")
	}
	if st.FillTruncatedTicks != 0 {
		t.Errorf("持续时长应归零，got %d", st.FillTruncatedTicks)
	}
	if st.FillUnresolved != 0 {
		t.Errorf("没轮到的成员数应归零，got %d", st.FillUnresolved)
	}
	if st.FillTruncatedTotal == 0 {
		t.Error("累计数不该被清掉——事后复盘要靠它")
	}
}

// 正常情形（两三轮就收敛）不许误报截断，否则这个指标会被当噪音忽略。
func TestNormalFillReportsNoTruncation(t *testing.T) {
	s := newSched(10_000_000)
	for i := 0; i < 20; i++ {
		backlogged(memberFor(s, "u"+itoa(i), 0), 1<<20)
	}
	s.recompute()
	st := s.Status()
	if st.FillTruncated {
		t.Error("20 个同质成员一轮就收敛，不该报截断")
	}
	// 同质成员谁也钉不住：第一轮就走「无人新饱和 → 按权重分完」，完成 0 轮注水。
	// 0 与上限 8 一眼可分，这正是这个字段要提供的信号。
	if st.FillRounds != 0 {
		t.Errorf("同质成员第一轮就分完，want 0 轮，got %d", st.FillRounds)
	}
	if st.FillTruncatedTotal != 0 {
		t.Errorf("累计截断应为 0，got %d", st.FillTruncatedTotal)
	}
}

// 截断了也照样守住总额——这条不能因为「近似分配」而松掉。
func TestTruncatedFillStillRespectsRootCap(t *testing.T) {
	const root = uint64(100_000_000)
	s := truncatingSched(t, root, fairFillMaxRounds+4)
	s.recompute()
	if !s.Status().FillTruncated {
		t.Fatal("前提不成立：应该先截断")
	}
	var total uint64
	for _, m := range s.members {
		total += m.alloc
	}
	if total > root {
		t.Errorf("截断后 Σ allocation = %d > root_cap %d", total, root)
	}
}

// 不拥塞时也要报状态：Status 是运维查「为什么分配看起来不对」的入口，
// 第一个要排除的就是「其实根本没进公平模式」。
func TestStatusReportsCongestionMode(t *testing.T) {
	s := newSched(1_000_000)
	s.SetCongestionHysteresis(80, 60, 3)
	a := memberFor(s, "a", 0)
	b := memberFor(s, "b", 0)

	backlogged(a, 300_000)
	backlogged(b, 300_000)
	s.recompute() // 利用率 60% < 80%
	if st := s.Status(); st.Congested {
		t.Error("60% 利用率不该是公平模式")
	}

	backlogged(a, 450_000)
	backlogged(b, 450_000)
	s.recompute() // 利用率 90%
	st := s.Status()
	if !st.Congested {
		t.Error("90% 利用率应进公平模式")
	}
	if st.RootCapBytePerSec != 1_000_000 || st.ActiveMembers != 2 {
		t.Errorf("root_cap / 活跃成员数没报对：%+v", st)
	}
}

// 同样的节点状态必须算出同样的分配。
//
// 这条是被上面那个截断用例逼出来的：原来一轮之内是**边判边扣**——同一轮里排在
// 后面的成员看到的池子已经被前面的人扣小了。而活跃成员是从 map 里遍历出来的，
// 顺序每 tick 都不一样，于是同一批人同样的需求，这一秒和下一秒能算出不同的额度。
// 客户端表现为速率无缘无故抖，而日志里什么都看不出来。
// 现在一轮之内对着同一个 (pool, totalWeight) 快照判定，钉完再一次性扣。
func TestFillIsOrderIndependent(t *testing.T) {
	// 用会撞轮数上限的那套阶梯：它对「边判边扣」最敏感。
	s := truncatingSched(t, 100_000_000, fairFillMaxRounds+4)
	s.recompute()

	want := make(map[string]uint64, len(s.members))
	for email, m := range s.members {
		want[email] = m.alloc
	}

	for tick := 0; tick < 50; tick++ {
		for _, m := range s.members {
			backlogged(m, 1<<20)
		}
		s.recompute()
		for email, m := range s.members {
			if m.alloc != want[email] {
				t.Fatalf("第 %d 个 tick 上 %s 的额度变了：%d → %d（输入一个字节都没变，"+
					"这就是遍历顺序在影响分配）", tick+1, email, want[email], m.alloc)
			}
		}
	}
}
