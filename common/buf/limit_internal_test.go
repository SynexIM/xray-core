package buf

import (
	"context"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// burst = 1/8 秒配额（125ms 窗口），floor 到单次读缓冲 Size——锯齿修复的核心参数。
func TestRateLimitBurstWindow(t *testing.T) {
	if got := rateLimitBurst(1_000_000); got != 125_000 {
		t.Errorf("burst(1MB/s): want 125000, got %d", got)
	}
	// 小限速值（16KB/s）：1/8 配额 = 2KB < Size(8KB) → floor 到 Size，
	// 保证 burst 不小于单次读缓冲，不会永久饿死。
	if got := rateLimitBurst(16 * 1024); got != Size {
		t.Errorf("burst(16KB/s): want Size %d, got %d", Size, got)
	}
}

// total > burst 时分片循环取令牌，不报错不丢字节。
func TestRateLimitWaitNChunksAboveBurst(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1_000_000_000), Size)
	if err := rateLimitWaitN(context.Background(), limiter, 10*Size); err != nil {
		t.Fatalf("chunked waitN: %v", err)
	}
}

// ctx 绑定：连接 ctx 取消后 WaitN 立即返回，不再睡在共享桶上占配额。
func TestRateLimitWaitNCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	limiter := rate.NewLimiter(rate.Limit(1), 1) // 1 B/s：不取消则要等十万秒
	start := time.Now()
	err := rateLimitWaitN(ctx, limiter, 100_000)
	if err == nil {
		t.Fatal("want error from canceled ctx")
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("canceled waitN must return immediately, took %v", elapsed)
	}
}

// 并发 SetBurst 缩小窗口时按新 burst 重试（只慢不断连），不把 WaitN 错误上抛断连。
func TestRateLimitWaitNRetriesAfterBurstShrink(t *testing.T) {
	limiter := rate.NewLimiter(rate.Limit(1_000_000_000), 64*1024)
	done := make(chan error, 1)
	go func() {
		// 模拟调度器在 waitN 循环中途缩小 burst
		time.Sleep(5 * time.Millisecond)
		limiter.SetBurst(Size)
		done <- nil
	}()
	if err := rateLimitWaitN(context.Background(), limiter, 512*1024); err != nil {
		t.Fatalf("waitN across burst shrink: %v", err)
	}
	<-done
}

// 平滑性（锯齿根因回归）：burst 只覆盖 1/8 秒，超过 burst 的量必须按速率排队。
// 100KB/s 限速下取 50KB：首个 burst 12.5KB 免等，其余 37.5KB ≈ 375ms。
// 若 burst 仍是整秒配额（旧行为），50KB < 100KB burst → 0 等待，此断言会抓住回归。
func TestRateLimitPacingNoFullSecondBurst(t *testing.T) {
	limiter, _ := NewRateLimiter(100_000)
	start := time.Now()
	if err := rateLimitWaitN(context.Background(), limiter, 50_000); err != nil {
		t.Fatalf("waitN: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 250*time.Millisecond {
		t.Errorf("50KB at 100KB/s with 1/8s burst must pace ≈375ms, finished in %v (full-second burst regression?)", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Errorf("pacing too slow: %v", elapsed)
	}
}
