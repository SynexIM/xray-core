package protocol

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 不挤时：起步跑突发，额度花完稳定在标准，之后一直是标准——持续速率不是上限，不许降。
// 同时量满载下新连接的首字节等待（≤ 一个量子）。
func TestPoolShaperUncongestedBurstThenStandardNeverSustained(t *testing.T) {
	const mb = 1 << 20
	s := newTierShaper()
	s.Configure(TierPolicy{Standard: 4 * mb, Burst: 16 * mb, BurstCredit: 12 * mb, Sustained: 1 * mb})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var granted atomic.Int64
	var bulk sync.WaitGroup
	for range 4 {
		bulk.Add(1)
		go func() {
			defer bulk.Done()
			f := s.NewFlow()
			for f.Wait(ctx, 16<<10) == nil {
				granted.Add(16 << 10)
			}
		}()
	}
	start := time.Now()
	window := func(from, to time.Duration) float64 {
		time.Sleep(time.Until(start.Add(from)))
		b0, t0 := granted.Load(), time.Now()
		time.Sleep(time.Until(start.Add(to)))
		return float64(granted.Load()-b0) / time.Since(t0).Seconds()
	}
	check := func(name string, got, want float64) {
		errPct := (got - want) / want * 100
		t.Logf("%-9s want %5.2f MB/s got %5.2f MB/s (%+.1f%%)", name, want/mb, got/mb, errPct)
		if errPct > 10 || errPct < -10 {
			t.Errorf("%s rate off by %.1f%%", name, errPct)
		}
	}
	check("burst", window(100*time.Millisecond, 800*time.Millisecond), 16*mb)
	check("standard", window(1500*time.Millisecond, 3000*time.Millisecond), 4*mb)
	var firstByte time.Duration
	for range 5 {
		begin := time.Now()
		if err := s.NewFlow().Wait(ctx, 1500); err != nil {
			t.Fatal(err)
		}
		firstByte = max(firstByte, time.Since(begin))
		time.Sleep(50 * time.Millisecond)
	}
	check("still-std", window(3500*time.Millisecond, 4500*time.Millisecond), 4*mb)
	if firstByte > tierQuantum {
		t.Errorf("new connection waited %v for its first byte under load", firstByte)
	}
	cancel()
	bulk.Wait()
}

// 同一个 pool 的不同用户对象（不同 email、不同入站）共用一套整形器；
// pool 不同就绝不共用，哪怕 email 一样。
func TestPoolIdentityIsExplicit(t *testing.T) {
	a := &MemoryUser{Email: "a@x", Pool: "p1", DownloadBandwidthBps: 8e6}
	b := &MemoryUser{Email: "b@x", Pool: "p1", DownloadBandwidthBps: 8e6}
	c := &MemoryUser{Email: "a@x", Pool: "p2", DownloadBandwidthBps: 8e6}
	_, da, ra := a.AcquireTierShapers()
	_, db, rb := b.AcquireTierShapers()
	_, dc, rc := c.AcquireTierShapers()
	defer ra()
	defer rb()
	defer rc()
	if da != db {
		t.Error("same pool must share one shaper")
	}
	if da == dc {
		t.Error("different pools must not share a shaper even with the same email")
	}
}

// 拥挤时：按 class 权重注水、低权重不低于地板、重度池排在正常池之后但保底持续速率，
// Σ 份额 ≤ root_cap；不挤时全部回到标准。
func TestCongestedFillWeightsFloorsAndHeavy(t *testing.T) {
	const mb = 1 << 20
	sched := nodeFairScheduler
	sched.started.Store(true) // 测试里手动 tick，不起后台 goroutine
	sched.SetNodeBandwidth(10 * mb)
	sched.SetCongestionHysteresis(90, 70, 1)
	sched.SetClassPolicies([]*ClassPolicy{
		{Name: "hi", Weight: 3},
		{Name: "lo", Weight: 1, FloorBytePerSec: 3 * mb},
		{Name: "hv", Weight: 1, HeavyWindowSeconds: 15, HeavyPercent: 80},
	})
	t.Cleanup(func() {
		sched.SetNodeBandwidth(0)
		sched.SetCongestionHysteresis(0, 0, 0)
		sched.SetClassPolicies(nil)
	})
	user := func(pool, class string) (*TierShaper, func()) {
		u := &MemoryUser{Pool: pool, Class: class, DownloadBandwidthBps: 8 * 8 * mb, SustainedBitPerSec: 8 * 1 * mb}
		_, down, release := u.AcquireTierShapers()
		return down, release
	}
	hi, r1 := user("t-hi", "hi")
	lo, r2 := user("t-lo", "lo")
	defer r1()
	defer r2()
	// 两个都满载且在排队。
	load := func(shapers ...*TierShaper) {
		for _, s := range shapers {
			s.mu.Lock()
			s.granted += 10 * mb
			s.waits++
			s.mu.Unlock()
		}
		sched.recompute()
	}
	share := func(s *TierShaper) uint64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		return uint64(s.share)
	}
	load(hi, lo)
	load(hi, lo)
	// root 10：lo 地板 3，剩 7 按 3:1 → hi 3+… 实际 hi=0+7*3/4=5.25，lo=3+7/4=4.75。
	// 地板保证 lo ≥ 3；没有地板时 3:1 是 7.5/2.5。
	if h, l := share(hi), share(lo); l < 3*mb || h+l > 10*mb || h <= l {
		t.Fatalf("weighted fill with floor: hi=%.2f lo=%.2f MB/s", float64(h)/mb, float64(l)/mb)
	}
	sched.SetClassPolicies([]*ClassPolicy{{Name: "hi", Weight: 3}, {Name: "lo", Weight: 1}, {Name: "hv", Weight: 1, HeavyWindowSeconds: 15, HeavyPercent: 80}})
	load(hi, lo)
	if h, l := share(hi), share(lo); h != 3*l || h+l > 10*mb {
		t.Fatalf("3:1 without floors: hi=%d lo=%d", h, l)
	}

	// 重度池：标准 8，持续 1。满载 15 秒后被识别为重度，拥挤时排到正常池之后，只拿保底。
	hv, r3 := user("t-hv", "hv")
	defer r3()
	for range 16 {
		load(hi, lo, hv)
	}
	if got := sched.Status().HeavyMembers; got != 1 {
		t.Fatalf("heavy members = %d, want 1", got)
	}
	if v := share(hv); v != 1*mb {
		t.Fatalf("heavy pool must be held at its sustained rate while normals want everything, got %.2f MB/s", float64(v)/mb)
	}

	// 不挤了：全部回到标准（setShare(false)），不留拥挤态份额。
	for range 3 {
		sched.recompute()
	}
	for _, s := range []*TierShaper{hi, lo, hv} {
		if rate, _ := s.Snapshot(); rate != 8*mb {
			t.Fatalf("uncongested pool must run at standard, got %.2f MB/s", rate/mb)
		}
	}
}
