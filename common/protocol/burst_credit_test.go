package protocol

import "testing"

func burstPolicy() *ClassPolicy {
	return &ClassPolicy{
		Name:                "burst",
		NormalCapBytePerSec: 2_500_000,  // 20 Mbps 基准
		BurstCapBytePerSec:  15_000_000, // 120 Mbps 峰值
		BurstCreditBytes:    1 << 30,    // 约 1 GB
	}
}

// 头一桶信用是满的，峰值就是 burst_cap —— 「打开网页觉得线路很快」。
func TestFreshCreditRunsAtBurstCap(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)
	if got := c.ceilingBytePerSec(p); got != p.BurstCapBytePerSec {
		t.Fatalf("want %d, got %d", p.BurstCapBytePerSec, got)
	}
}

// FR-078 的核心：credit 只按「超出 normal_cap 的那部分字节」扣。
// 跑 120 Mbps、基准 20 Mbps 时按 100 Mbps 的量消耗，不是 120。
func TestCreditChargesOnlyTheExcessOverNormalCap(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)
	full := c.bytes

	c.settle(p, p.BurstCapBytePerSec, 1000) // 跑满一秒峰值
	spent := full - c.bytes
	want := p.BurstCapBytePerSec - p.NormalCapBytePerSec
	if spent != want {
		t.Fatalf("扣了 %d 字节，应该只扣超出基准的 %d 字节（按全量扣的话是 %d）",
			spent, want, p.BurstCapBytePerSec)
	}
}

// 跑在基准速度以内的人一分信用都不该被扣 —— 老实人不该因为一直在用而被罚。
func TestRunningAtOrBelowNormalCapCostsNothing(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)
	c.bytes = p.BurstCreditBytes / 2 // 先花掉一半
	before := c.bytes
	c.settle(p, p.NormalCapBytePerSec, 1000)
	if c.bytes < before {
		t.Fatalf("跑基准速度反被扣：before %d, after %d", before, c.bytes)
	}
}

// 空闲回补：不用的时候按没跑满的差额攒回来，攒满就停。
func TestIdleRefillsAndCapsAtCapacity(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)
	c.bytes = 0
	c.settle(p, 0, 1000)
	if c.bytes != p.NormalCapBytePerSec {
		t.Fatalf("空闲一秒应回补 %d，got %d", p.NormalCapBytePerSec, c.bytes)
	}
	for i := 0; i < 1000; i++ {
		c.settle(p, 0, 1000)
	}
	if c.bytes != p.BurstCreditBytes {
		t.Fatalf("回补必须封顶在桶容量 %d，got %d", p.BurstCreditBytes, c.bytes)
	}
}

// 峰值随信用线性衰减：信用满 → burst_cap，信用空 → normal_cap，中间线性。
// 不做「信用一没就断崖掉回基准」，那在客户端表现为下载突然卡死一下。
func TestPeakDecaysLinearlyWithCredit(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)

	c.bytes = c.capacity / 2
	mid := c.ceilingBytePerSec(p)
	want := p.NormalCapBytePerSec + (p.BurstCapBytePerSec-p.NormalCapBytePerSec)/2
	if mid != want {
		t.Fatalf("半桶信用: want %d, got %d", want, mid)
	}

	c.bytes = 0
	if got := c.ceilingBytePerSec(p); got != p.NormalCapBytePerSec {
		t.Fatalf("信用花光应回落到基准 %d, got %d", p.NormalCapBytePerSec, got)
	}
}

// 持续拉大流量的人一定会稳定回落到基准 —— 这就是「不做测速站识别」也能成立的理由：
// 偶尔跑一次测速很快，一直猛拉的人自然掉回来。
func TestSustainedHeavyUseSettlesBackToNormalCap(t *testing.T) {
	p := burstPolicy()
	var c burstCredit
	c.settle(p, 0, 0)
	ticks := 0
	for c.ceilingBytePerSec(p) > p.NormalCapBytePerSec {
		c.settle(p, c.ceilingBytePerSec(p), 1000)
		ticks++
		if ticks > 10_000 {
			t.Fatal("一直猛拉却永远掉不回基准 —— 那 normal_cap 就成摆设了")
		}
	}
	t.Logf("持续以当前峰值下载，%d 秒后回落到基准 %d B/s", ticks, p.NormalCapBytePerSec)
}

// 没配突发（burst_cap 不高于 normal_cap，或没给信用）就当没这回事，
// 上限直接是 normal_cap，不许凭空多给。
func TestNoBurstPolicyMeansNormalCapOnly(t *testing.T) {
	cases := []*ClassPolicy{
		{NormalCapBytePerSec: 2_500_000}, // 没给信用
		{NormalCapBytePerSec: 2_500_000, BurstCapBytePerSec: 2_000_000, BurstCreditBytes: 1 << 30}, // burst 比基准还低
	}
	for _, p := range cases {
		var c burstCredit
		c.settle(p, 0, 0)
		if got := c.ceilingBytePerSec(p); got != p.NormalCapBytePerSec {
			t.Errorf("want %d, got %d", p.NormalCapBytePerSec, got)
		}
	}
	var c burstCredit
	if got := c.ceilingBytePerSec(nil); got != 0 {
		t.Errorf("无策略 = 无 class 上限（0），got %d", got)
	}
}

// 调度器层面：突发信用真的影响成员的天花板，而且只在不拥塞时冲得上去。
func TestSchedulerCeilingFollowsBurstCredit(t *testing.T) {
	s := newSched(100_000_000)
	p := burstPolicy()
	s.SetClassPolicies([]*ClassPolicy{p})
	m := memberIn(s, "b0", "burst", 0)
	if got := s.ceilingFor(m, 100_000_000); got != p.BurstCapBytePerSec {
		t.Fatalf("满信用天花板应是 %d, got %d", p.BurstCapBytePerSec, got)
	}
	m.credit.bytes = 0
	if got := s.ceilingFor(m, 100_000_000); got != p.NormalCapBytePerSec {
		t.Fatalf("信用花光天花板应是 %d, got %d", p.NormalCapBytePerSec, got)
	}
}

// recompute 每 tick 都替所有成员结算信用，空闲的人也不例外（他们正是要回补的人）。
func TestRecomputeSettlesCreditForIdleMembers(t *testing.T) {
	s := newSched(100_000_000)
	p := burstPolicy()
	s.SetClassPolicies([]*ClassPolicy{p})
	m := memberIn(s, "b0", "burst", 0)
	m.credit.bytes = 0
	s.recompute()
	if m.credit.bytes != p.NormalCapBytePerSec {
		t.Fatalf("空闲成员应在 recompute 里回补 %d, got %d", p.NormalCapBytePerSec, m.credit.bytes)
	}
}
