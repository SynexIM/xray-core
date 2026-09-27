package protocol

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"
)

// 限速层附加延迟：同一个池被一条满载连接打满时，另一条连接上的小包（游戏/交互）与
// 稳定中速流（直播推流）在整形器里要等多久。目标：小包 p50 ≤ 1ms、p99 ≤ 3ms；
// 推流每片的等待 p99 ≤ 3ms（抖动不因限速变大）。
//
// 这是防静默回退的门禁：量子、稀疏判定或欠账上限被改坏时，这里的分位数会先变。

type latencyStats struct{ p50, p99, max time.Duration }

func percentiles(xs []time.Duration) latencyStats {
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	at := func(p float64) time.Duration { return xs[min(len(xs)-1, int(p*float64(len(xs))))] }
	return latencyStats{p50: at(0.5), p99: at(0.99), max: xs[len(xs)-1]}
}

// saturate 起 n 条满载连接，按 buf.Size 的读粒度不停取令牌。
func saturate(ctx context.Context, s *TierShaper, n, readBytes int) *sync.WaitGroup {
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := s.NewFlow()
			for f.Wait(ctx, readBytes) == nil {
			}
		}()
	}
	return &wg
}

// paced 在一条连接上每 interval 发一片 size 字节，记每片在整形器里的等待。
func paced(ctx context.Context, s *TierShaper, size int, interval time.Duration, count int) []time.Duration {
	f := s.NewFlow()
	waits := make([]time.Duration, 0, count)
	next := time.Now()
	for range count {
		next = next.Add(interval)
		time.Sleep(time.Until(next))
		begin := time.Now()
		if err := f.Wait(ctx, size); err != nil {
			break
		}
		waits = append(waits, time.Since(begin))
	}
	return waits
}

func TestTierShaperAddedLatencyUnderLoad(t *testing.T) {
	const mbit = 1_000_000 / 8
	for _, tc := range []struct {
		name      string
		rate      uint64 // byte/s
		congested uint64 // >0: 节点拥塞，池份额压到这个值
		stream    uint64 // 推流速率 byte/s，须低于它在池里的公平份额（高于份额就是被限速，不是抖动）
		bulk      int    // 满载连接数
	}{
		// 独占池的一条满载连接自己也算稀疏（份额 = 整池）：它的大片不能挡住小包。
		{name: "20M pool one bulk flow", rate: 20 * mbit, stream: 5 * mbit, bulk: 1},
		{name: "20M pool saturated", rate: 20 * mbit, stream: 5 * mbit},
		{name: "100M pool saturated", rate: 100 * mbit, stream: 5 * mbit},
		{name: "congested heavy pool 10M share", rate: 100 * mbit, congested: 10 * mbit, stream: 2 * mbit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTierShaper()
			s.Configure(TierPolicy{Standard: tc.rate})
			if tc.congested > 0 {
				s.setShare(true, tc.congested)
			}
			ctx, cancel := context.WithCancel(context.Background())
			flows := tc.bulk
			if flows == 0 {
				flows = 2
			}
			bulk := saturate(ctx, s, flows, 64<<10)
			time.Sleep(200 * time.Millisecond)

			var ping, stream []time.Duration
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); ping = paced(ctx, s, 120, 10*time.Millisecond, 200) }()
			// 推流：每 10ms 一片。
			go func() {
				defer wg.Done()
				stream = paced(ctx, s, int(tc.stream/100), 10*time.Millisecond, 200)
			}()
			wg.Wait()
			cancel()
			bulk.Wait()

			p, st := percentiles(ping), percentiles(stream)
			t.Logf("ping   p50=%v p99=%v max=%v", p.p50, p.p99, p.max)
			t.Logf("stream p50=%v p99=%v max=%v", st.p50, st.p99, st.max)
			if p.p50 > time.Millisecond || p.p99 > 3*time.Millisecond {
				t.Errorf("small packets wait in the shaper: p50=%v p99=%v", p.p50, p.p99)
			}
			if st.p99 > 3*time.Millisecond {
				t.Errorf("a steady below-share stream is jittered by the shaper: p99=%v", st.p99)
			}
		})
	}
}

// 拥挤时没排队的池（打网页、游戏）不按上一秒实测钉死：份额抬到同层水位，
// 否则一次突发要等下一 tick 才拿得到带宽。
func TestCongestedSatisfiedPoolIsRaisedToWaterLevel(t *testing.T) {
	const mb = 1 << 20
	sched := nodeFairScheduler
	sched.started.Store(true)
	sched.SetNodeBandwidth(10 * mb)
	sched.SetCongestionHysteresis(0, 0, 0)
	t.Cleanup(func() { sched.SetNodeBandwidth(0) })
	user := func(pool string) (*TierShaper, func()) {
		u := &MemoryUser{Pool: pool, DownloadBandwidthBps: 8 * 8 * mb}
		_, down, release := u.AcquireTierShapers()
		return down, release
	}
	bulkA, r1 := user("lvl-bulk-a")
	bulkB, r2 := user("lvl-bulk-b")
	light, r3 := user("lvl-light")
	defer r1()
	defer r2()
	defer r3()
	for range 2 {
		for _, s := range []*TierShaper{bulkA, bulkB} {
			s.mu.Lock()
			s.granted += 10 * mb
			s.waits++
			s.mu.Unlock()
		}
		light.mu.Lock()
		light.granted += 64 << 10
		light.mu.Unlock()
		sched.recompute()
	}
	light.mu.Lock()
	share := light.share
	light.mu.Unlock()
	if share < 3*mb {
		t.Fatalf("light pool pinned to its last-second usage: %.2f MB/s, want the water level (~5 MB/s)", share/mb)
	}
}
