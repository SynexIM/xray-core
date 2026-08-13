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
	if err := rateLimitWaitN(context.Background(), []*rate.Limiter{limiter}, 10*Size); err != nil {
		t.Fatalf("chunked waitN: %v", err)
	}
}

// ctx 绑定：连接 ctx 取消后 WaitN 立即返回，不再睡在共享桶上占配额。
func TestRateLimitWaitNCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	limiter := rate.NewLimiter(rate.Limit(1), 1) // 1 B/s：不取消则要等十万秒
	start := time.Now()
	err := rateLimitWaitN(ctx, []*rate.Limiter{limiter}, 100_000)
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
	if err := rateLimitWaitN(context.Background(), []*rate.Limiter{limiter}, 512*1024); err != nil {
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
	if err := rateLimitWaitN(context.Background(), []*rate.Limiter{limiter}, 50_000); err != nil {
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

// 串联语义：每一批字节要**依次通过每一个桶**，所以只要链条里有一个慢桶，
// 整体就被它压住。这里把慢桶放在第二位——如果实现只取了第一个桶（回归到单速率），
// 这段会瞬间返回，断言当场抓住。
func TestRateLimitWaitNHonoursEveryBucketInChain(t *testing.T) {
	fast, _ := NewRateLimiter(1_000_000_000) // 1GB/s，基本不构成约束
	slow, _ := NewRateLimiter(100_000)       // 100KB/s，burst 12.5KB
	start := time.Now()
	if err := rateLimitWaitN(context.Background(), []*rate.Limiter{fast, slow}, 50_000); err != nil {
		t.Fatalf("waitN: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("链条里的慢桶被跳过了：50KB 只花了 %v，期望 ≈375ms", elapsed)
	}
}

// 顺序无关：慢桶放第一位同样要生效。
func TestRateLimitWaitNHonoursFirstBucketToo(t *testing.T) {
	slow, _ := NewRateLimiter(100_000)
	fast, _ := NewRateLimiter(1_000_000_000)
	start := time.Now()
	if err := rateLimitWaitN(context.Background(), []*rate.Limiter{slow, fast}, 50_000); err != nil {
		t.Fatalf("waitN: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Errorf("链条首位的慢桶被跳过了：50KB 只花了 %v，期望 ≈375ms", elapsed)
	}
}

// compactLimiters：调用方可以无脑传「峰值桶, 承诺桶」，没配承诺速率时后者是 nil。
// 全是 nil 时必须原样返回底层 reader/writer，不套一层空壳。
func TestRateLimitWrappersDropNilLimiters(t *testing.T) {
	limiter, _ := NewRateLimiter(100_000)
	reader := &MultiBufferContainer{}
	if got := NewRateLimitReaderWithLimiter(context.Background(), reader, nil, nil); got != Reader(reader) {
		t.Error("全 nil 时不该包一层限速壳")
	}
	wrapped := NewRateLimitReaderWithLimiter(context.Background(), reader, nil, limiter)
	rl, ok := wrapped.(*RateLimitReader)
	if !ok {
		t.Fatalf("期望 *RateLimitReader，得到 %T", wrapped)
	}
	if len(rl.limiters) != 1 || rl.limiters[0] != limiter {
		t.Errorf("nil 没被丢掉：limiters = %v", rl.limiters)
	}
}
