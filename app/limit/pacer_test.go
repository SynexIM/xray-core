package limit

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestPacerMultiBucketDoesNotAllocateAfterAttach(t *testing.T) {
	first, second := newShaper(), newShaper()
	defer first.close()
	defer second.close()
	first.Configure(ladder(100_000_000_000))
	second.Configure(ladder(100_000_000_000))
	pacer := newConnectionPacer([]*Flow{first.NewFlow(), second.NewFlow()}, nil, nil, nil, false)
	defer pacer.close()
	allocations := testing.AllocsPerRun(1000, func() {
		if err := pacer.Wait(context.Background(), 64, false); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("多桶逐包分配: %f", allocations)
	}
}
func TestMultiBucketOppositeSparseClassDoesNotDeadlock(t *testing.T) {
	first, second := newShaper(), newShaper()
	defer first.close()
	defer second.close()
	first.Configure(ladder(8_000_000))
	second.Configure(ladder(8_000_000))
	left := newConnectionPacer([]*Flow{first.NewFlow(), second.NewFlow()}, nil, nil, nil, false)
	right := newConnectionPacer([]*Flow{second.NewFlow(), first.NewFlow()}, nil, nil, nil, false)
	defer left.close()
	defer right.close()
	first.mu.Lock()
	left.flows[0].used = 50000
	first.mu.Unlock()
	second.mu.Lock()
	right.flows[1].used = 50000
	second.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var wait sync.WaitGroup
	for _, pacer := range []*connectionPacer{left, right} {
		wait.Add(1)
		go func(pacer *connectionPacer) {
			defer wait.Done()
			for packet := 0; packet < 20; packet++ {
				if err := pacer.Wait(ctx, 1500, false); err != nil {
					t.Errorf("多桶没有前进: %v", err)
					return
				}
			}
		}(pacer)
	}
	wait.Wait()
	if first.granted != 60000 || second.granted != 60000 {
		t.Fatalf("多桶记账不一致: %d %d", first.granted, second.granted)
	}
}
func TestDatagramDebtNeverExceedsOneQuantum(t *testing.T) {
	shaper := newShaper()
	defer shaper.close()
	shaper.Configure(ladder(8_000_000, 4_000_000))
	flow := shaper.NewFlow()
	defer flow.release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for packet := 0; packet < 100; packet++ {
		if err := flow.wait(ctx, 1200, true); err != nil {
			t.Fatal(err)
		}
		shaper.mu.Lock()
		if shaper.tokens < -shaper.quantumBytes() {
			shaper.mu.Unlock()
			t.Fatal("方向桶预借超过一个量子")
		}
		for _, bucket := range shaper.buckets {
			if bucket.tokens < -quantumBytesAt(bucket.rate) {
				shaper.mu.Unlock()
				t.Fatal("阶梯桶预借超过一个量子")
			}
		}
		shaper.mu.Unlock()
	}
}
