package protocol

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 真实时钟下同时量三件事：各档速率误差、满载时新连接首字节时延、满载时稀疏连接的 RTT 增量。
func TestTierShaperRatesAndLatencyUnderLoad(t *testing.T) {
	const mb = 1 << 20
	s := newTierShaper()
	s.Configure(TierPolicy{
		Standard: 4 * mb, Burst: 16 * mb, BurstCredit: 12 * mb,
		Sustained: 2 * mb, SustainedAfter: 2 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var granted atomic.Int64
	var perFlow [4]atomic.Int64
	var paused atomic.Bool
	var bulk sync.WaitGroup
	for i := range 4 {
		bulk.Add(1)
		go func() {
			defer bulk.Done()
			f := s.NewFlow()
			for ctx.Err() == nil {
				if paused.Load() {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if f.Wait(ctx, 16<<10) != nil {
					return
				}
				granted.Add(16 << 10)
				perFlow[i].Add(16 << 10)
			}
		}()
	}

	var probeMax atomic.Int64
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		f := s.NewFlow()
		for ctx.Err() == nil {
			time.Sleep(50 * time.Millisecond)
			begin := time.Now()
			if f.Wait(ctx, 200) != nil {
				return
			}
			if d := int64(time.Since(begin)); d > probeMax.Load() {
				probeMax.Store(d)
			}
		}
	}()

	start := time.Now()
	window := func(from, to time.Duration) float64 {
		time.Sleep(time.Until(start.Add(from)))
		b0, t0 := granted.Load(), time.Now()
		time.Sleep(time.Until(start.Add(to)))
		return float64(granted.Load()-b0) / time.Since(t0).Seconds()
	}
	check := func(name string, got, want float64) {
		errPct := (got - want) / want * 100
		t.Logf("%-10s want %6.2f MB/s  got %6.2f MB/s  error %+5.1f%%", name, want/mb, got/mb, errPct)
		if errPct > 10 || errPct < -10 {
			t.Errorf("%s rate off by %.1f%%", name, errPct)
		}
	}

	check("burst", window(100*time.Millisecond, 800*time.Millisecond), 16*mb)
	check("standard", window(1500*time.Millisecond, 2800*time.Millisecond), 4*mb)

	var firstByteMax time.Duration
	for range 5 {
		begin := time.Now()
		if err := s.NewFlow().Wait(ctx, 1500); err != nil {
			t.Fatal(err)
		}
		firstByteMax = max(firstByteMax, time.Since(begin))
		time.Sleep(100 * time.Millisecond)
	}
	check("sustained", window(3600*time.Millisecond, 4800*time.Millisecond), 2*mb)

	// 负载降下来后退出持续档。
	paused.Store(true)
	time.Sleep(time.Until(start.Add(4800*time.Millisecond + tierRecoverWindow + 200*time.Millisecond)))
	if _, _, sustained := s.Snapshot(); sustained {
		t.Error("sustained tier did not recover after load stopped")
	}
	paused.Store(false)

	// 改档不断连：同一批连接上直接换策略。
	s.Configure(TierPolicy{Standard: 8 * mb})
	check("retiered", window(6300*time.Millisecond, 7200*time.Millisecond), 8*mb)

	probeRTT := time.Duration(probeMax.Load())
	t.Logf("new connection first byte max %v, sparse connection RTT increase max %v (quantum %v)", firstByteMax, probeRTT, tierQuantum)
	if firstByteMax > tierQuantum {
		t.Errorf("new connection waited %v for its first byte", firstByteMax)
	}
	if probeRTT > tierQuantum+5*time.Millisecond {
		t.Errorf("sparse connection RTT increased by %v under load", probeRTT)
	}

	cancel()
	bulk.Wait()
	<-probeDone
	lo, hi := perFlow[0].Load(), perFlow[0].Load()
	for i := range perFlow {
		lo, hi = min(lo, perFlow[i].Load()), max(hi, perFlow[i].Load())
	}
	t.Logf("per-connection share max/min %.3f", float64(hi)/float64(lo))
	if float64(hi)/float64(lo) > 1.1 {
		t.Errorf("connections of one user unfair: max/min %.3f", float64(hi)/float64(lo))
	}
}

// 受管用户起初不限速，改成限速后同一条已建连接必须立即受限（不靠重连）。
func TestTierShaperUnlimitedToLimitedOnLiveFlow(t *testing.T) {
	u := &MemoryUser{Email: "seam@test", EgressTag: "egress"}
	_, down, release := u.AcquireTierShapers()
	defer release()
	flow := down.NewFlow()
	if err := flow.Wait(context.Background(), 8<<20); err != nil {
		t.Fatal(err)
	}
	limited := *u
	limited.DownloadBandwidthBps, limited.UploadBandwidthBps = 8*256<<10, 8*256<<10 // 256 KiB/s
	ApplyTierPolicy(&limited)
	begin := time.Now()
	if err := flow.Wait(context.Background(), 256<<10); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(begin); took < 800*time.Millisecond {
		t.Fatalf("256 KiB at 256 KiB/s took %v; the live flow was not re-limited", took)
	}
}
