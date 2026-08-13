package buf

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// RateLimitReader throttles payload reads to roughly bytesPerSecond.
// A zero limit leaves traffic untouched.
type RateLimitReader struct {
	Reader
	ctx     context.Context
	limiter *rate.Limiter
}

type RateLimitWriter struct {
	Writer
	ctx     context.Context
	limiter *rate.Limiter
}

func NewRateLimitReader(ctx context.Context, reader Reader, bytesPerSecond uint64) Reader {
	if bytesPerSecond == 0 {
		return reader
	}
	limiter, _ := NewRateLimiter(bytesPerSecond)
	return NewRateLimitReaderWithLimiter(ctx, reader, limiter)
}

func NewRateLimiter(bytesPerSecond uint64) (*rate.Limiter, int) {
	burst := rateLimitBurst(bytesPerSecond)
	return rate.NewLimiter(rate.Limit(bytesPerSecond), burst), burst
}

// NewRateLimitReaderWithLimiter wraps reader with a shared token bucket.
// ctx 绑定连接生命周期：连接关闭后 WaitN 立即返回，不再睡在（与同用户其他连接
// 共享的）桶上占配额（消除共享桶 FIFO 队头阻塞的最坏形态）。ctx 为 nil 时退化为
// Background（不取消，仅兜底，调用方应传连接 ctx）。
func NewRateLimitReaderWithLimiter(ctx context.Context, reader Reader, limiter *rate.Limiter) Reader {
	if limiter == nil {
		return reader
	}
	return &RateLimitReader{
		Reader:  reader,
		ctx:     ctx,
		limiter: limiter,
	}
}

func NewRateLimitWriterWithLimiter(ctx context.Context, writer Writer, limiter *rate.Limiter) Writer {
	if limiter == nil {
		return writer
	}
	return &RateLimitWriter{
		Writer:  writer,
		ctx:     ctx,
		limiter: limiter,
	}
}

// rateLimitBurst 由速率推 burst。burst = 1/8 秒配额（125ms 窗口）：此前 burst=整秒
// 配额会造成「1 秒突发打满 + 1 秒静默」的锯齿；125ms 窗口把突发压到人不可感知的
// 粒度。floor 到 Size（单次读缓冲 8KB），防小限速值（如 16KB/s）下 burst 过小导致
// 每次读都要多轮 WaitN（waitN 会按 burst 分片循环，功能上不会饿死，只是省循环）。
func rateLimitBurst(bytesPerSecond uint64) int {
	burst := int(bytesPerSecond / 8)
	if burst < Size {
		burst = Size
	}
	return burst
}

func (r *RateLimitReader) ReadMultiBuffer() (MultiBuffer, error) {
	return r.readWithTimeout(0, false)
}

func (r *RateLimitReader) ReadMultiBufferTimeout(timeout time.Duration) (MultiBuffer, error) {
	return r.readWithTimeout(timeout, true)
}

func (r *RateLimitReader) readWithTimeout(timeout time.Duration, withTimeout bool) (MultiBuffer, error) {
	var (
		mb  MultiBuffer
		err error
	)
	if withTimeout {
		timeoutReader, ok := r.Reader.(TimeoutReader)
		if !ok {
			return nil, ErrNotTimeoutReader
		}
		mb, err = timeoutReader.ReadMultiBufferTimeout(timeout)
	} else {
		mb, err = r.Reader.ReadMultiBuffer()
	}
	if mb.IsEmpty() {
		return mb, err
	}
	if waitErr := rateLimitWaitN(r.ctx, r.limiter, int(mb.Len())); waitErr != nil && err == nil {
		return mb, waitErr
	}
	return mb, err
}

func (w *RateLimitWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if mb.IsEmpty() {
		return w.Writer.WriteMultiBuffer(mb)
	}
	if err := rateLimitWaitN(w.ctx, w.limiter, int(mb.Len())); err != nil {
		return err
	}
	return w.Writer.WriteMultiBuffer(mb)
}

// rateLimitWaitN 按 limiter 当前 burst 分片阻塞取令牌（per-user 桶与节点公平桶共用）。
//   - ctx 绑定连接生命周期：连接关闭后立即带错返回，不再睡在共享桶上占队。
//   - burst 每轮动态读：节点公平调度器会并发 SetBurst 调整窗口；若在 Burst() 与
//     WaitN 之间 burst 被调小，WaitN(n>burst) 会立即报错——此时 ctx 仍存活则按新
//     burst 重试（n 单调收缩、必有进展），保证只慢不断连。
func rateLimitWaitN(ctx context.Context, limiter *rate.Limiter, total int) error {
	if ctx == nil {
		ctx = context.Background()
	}
	remaining := total
	for remaining > 0 {
		burst := limiter.Burst()
		if burst <= 0 {
			burst = Size
		}
		n := remaining
		if n > burst {
			n = burst
		}
		if err := limiter.WaitN(ctx, n); err != nil {
			if ctx.Err() != nil {
				return err
			}
			if n == 1 { // 非 burst 竞态的真实失败（如 limit=0），不再重试
				return err
			}
			continue
		}
		remaining -= n
	}
	return nil
}
