package limit

import (
	"context"
	"math"
	"sort"
	"sync"
	"testing"
	"time"
)

func ladder(rates ...uint64) *Ladder {
	result := &Ladder{}
	for i, rate := range rates {
		step := &Step{RateBps: rate}
		if i < len(rates)-1 {
			step.ForMs = 1000
		}
		result.Steps = append(result.Steps, step)
	}
	return result
}
func TestLadderIntersectionsAndIdleRecovery(t *testing.T) {
	s := newShaper()
	defer s.close()
	s.Configure(&Ladder{Steps: []*Step{{RateBps: 10_000_000, ForMs: 5000}, {RateBps: 4_000_000, ForMs: 20000}, {RateBps: 1_000_000}}})
	start := s.last
	flow := s.NewFlow()
	var perSecond [60]uint64
	for tick := 0; tick < 60000; tick++ {
		s.advance(start.Add(time.Duration(tick) * time.Millisecond))
		n := int(max(0, s.tokens))
		if n > 0 {
			s.grantLocked(flow, n)
			perSecond[tick/1000] += uint64(n)
		}
	}
	for second, bytes := range perSecond {
		expected := 10_000_000.0
		if second >= 5 {
			expected = 4_000_000
		}
		if second >= 25 {
			expected = 1_000_000
		}
		measured := float64(bytes) * 8
		if math.Abs(measured-expected)/expected > 0.06 {
			t.Fatalf("t=%ds rate=%.0f expected=%.0f", second+1, measured, expected)
		}
	}
	if len(s.buckets) != 3 || math.Abs(s.buckets[2].depth-(13_125_000+31_250)) > 1 {
		t.Fatalf("交点桶深错误: %+v", s.buckets)
	}
	total := uint64(0)
	for _, n := range perSecond {
		total += n
	}
	if float64(total) > 125_000*60+s.buckets[2].depth+1 {
		t.Fatal("长期平均突破到达曲线")
	}
	s.advance(start.Add(5 * time.Minute))
	for _, b := range s.buckets {
		if b.tokens != b.depth {
			t.Fatal("闲置后没有回满")
		}
	}
	rate, _ := s.ladderRate()
	if rate != 1_250_000 {
		t.Fatal("闲置后没有恢复首档")
	}
}
func TestProfileHotChangeRearmsExistingWaiter(t *testing.T) {
	s := newShaper()
	defer s.close()
	s.Configure(ladder(8))
	s.mu.Lock()
	s.tokens = -3000
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.NewFlow().Wait(ctx, 1500) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		queued := s.queue.Len() > 0
		s.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("等待者没有入队")
		}
		time.Sleep(time.Millisecond)
	}
	start := time.Now()
	s.Configure(ladder(8_000_000))
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("热改没有重拨旧定时器")
	}
}
func TestZeroCongestedShareDoesNotMeanUnlimited(t *testing.T) {
	s := newShaper()
	defer s.close()
	s.Configure(ladder(8_000_000))
	s.setShare(true, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.NewFlow().Wait(ctx, 1500); err != context.DeadlineExceeded {
		t.Fatalf("零份额越过整形: %v", err)
	}
	if s.granted != 0 || s.queue.Len() != 0 {
		t.Fatal("取消后留了放行或排队状态")
	}
}

func TestMultiBucketWaitStartsWithPausedStrictBucket(t *testing.T) {
	fast, paused := newShaper(), newShaper()
	defer fast.close()
	defer paused.close()
	fast.Configure(ladder(8_000_000))
	paused.Configure(ladder(8_000_000))
	paused.setShare(true, 0)
	pacer := &connectionPacer{flows: []*Flow{fast.NewFlow(), paused.NewFlow()}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pacer.Wait(ctx, 100, false); err != context.DeadlineExceeded {
		t.Fatalf("零份额没有阻塞: %v", err)
	}
	if fast.granted != 0 {
		t.Fatal("最严桶没放行却先扣了松桶")
	}
}
func TestSparseLatencyAndBulkByteFairness(t *testing.T) {
	baseline := newShaper()
	baselineFlow := baseline.NewFlow()
	baselineLatency := make([]time.Duration, 100)
	for i := range baselineLatency {
		start := time.Now()
		if err := baselineFlow.Wait(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
		baselineLatency[i] = time.Since(start)
	}
	baseline.close()
	sort.Slice(baselineLatency, func(i, j int) bool { return baselineLatency[i] < baselineLatency[j] })
	t.Logf("未限速基线 p50=%s p99=%s", baselineLatency[50], baselineLatency[99])
	s := newShaper()
	defer s.close()
	s.Configure(ladder(8_000_000))
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	totals := make([]uint64, 3)
	for i := range totals {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			flow := s.NewFlow()
			defer flow.release()
			for {
				if flow.Wait(ctx, 1500) != nil {
					return
				}
				totals[i] += 1500
			}
		}(i)
	}
	sparse := s.NewFlow()
	defer sparse.release()
	latency := make([]time.Duration, 0, 100)
	for range 100 {
		start := time.Now()
		if err := sparse.Wait(ctx, 100); err != nil {
			t.Fatal(err)
		}
		latency = append(latency, time.Since(start))
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	sort.Slice(latency, func(i, j int) bool { return latency[i] < latency[j] })
	p50, p99 := latency[50], latency[99]
	t.Logf("稀疏小包 p50=%s p99=%s，满载字节=%v", p50, p99, totals)
	if p50 > time.Millisecond || p99 > 3*time.Millisecond {
		t.Fatalf("稀疏延迟超标: p50=%s p99=%s", p50, p99)
	}
	for _, n := range totals {
		if n == 0 {
			t.Fatal("满载流被饿死")
		}
	}
	lo, hi := totals[0], totals[0]
	for _, n := range totals {
		lo = min(lo, n)
		hi = max(hi, n)
	}
	if float64(hi)/float64(lo) > 1.25 {
		t.Fatalf("满载流没有按字节公平: %v", totals)
	}
}
