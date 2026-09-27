package buf

import (
	"context"
	"math"
	"time"

	"github.com/xtls/xray-core/common"
	"golang.org/x/time/rate"
)

// RateLimitReader throttles payload reads to roughly bytesPerSecond.
// A zero limit leaves traffic untouched.
//
// limiters 是**串联**的桶，从松到紧：每一批字节必须依次通过每一个。单速率时只有
// 一个（峰值桶）；配了承诺速率时是两个，见 protocol.MemoryUser.RuntimeRateLimiters。
type RateLimitReader struct {
	Reader
	ctx      context.Context
	limiters []*rate.Limiter
	pacer    Pacer
}

type RateLimitWriter struct {
	Writer
	ctx      context.Context
	limiters []*rate.Limiter
	pacer    Pacer
}

// Pacer is a per-connection shaper that replaces the token-bucket chain; the
// pool shaper (protocol.TierFlow) implements it.
type Pacer interface {
	Wait(ctx context.Context, n int) error
}

// NewPacedReader wraps reader with a Pacer. A nil pacer leaves traffic untouched.
func NewPacedReader(ctx context.Context, reader Reader, pacer Pacer) Reader {
	if pacer == nil {
		return reader
	}
	return &RateLimitReader{Reader: reader, ctx: ctx, pacer: pacer}
}

// NewPacedWriter wraps writer with a Pacer. A nil pacer leaves traffic untouched.
func NewPacedWriter(ctx context.Context, writer Writer, pacer Pacer) Writer {
	if pacer == nil {
		return writer
	}
	return &RateLimitWriter{Writer: writer, ctx: ctx, pacer: pacer}
}

// 限速包装只管节流，关闭、打断与 XUDP 的「塞一个错误再恢复」都原样交给里面的管道：
// mux / XUDP 会话拿到的是包装后的 link，靠这些方法收尾；少转一个，会话就关不掉
// （XUDP 更是直接断言 *pipe.Reader，整个内核 panic）。
func (r *RateLimitReader) Interrupt() { common.Interrupt(r.Reader) }

func (r *RateLimitReader) Close() error { return common.Close(r.Reader) }

// ReturnAnError 与 Recover 见 pipe.Reader。
func (r *RateLimitReader) ReturnAnError(err error) {
	if p, ok := r.Reader.(interface{ ReturnAnError(error) }); ok {
		p.ReturnAnError(err)
	}
}

func (r *RateLimitReader) Recover() {
	if p, ok := r.Reader.(interface{ Recover() }); ok {
		p.Recover()
	}
}

func (w *RateLimitWriter) Interrupt() { common.Interrupt(w.Writer) }

func (w *RateLimitWriter) Close() error { return common.Close(w.Writer) }

func (r *RateLimitReader) wait(n int) error { return paceOrWait(r.ctx, r.pacer, r.limiters, n) }

func (w *RateLimitWriter) wait(n int) error { return paceOrWait(w.ctx, w.pacer, w.limiters, n) }

func paceOrWait(ctx context.Context, pacer Pacer, limiters []*rate.Limiter, n int) error {
	if pacer == nil {
		return rateLimitWaitN(ctx, limiters, n)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return pacer.Wait(ctx, n)
}

func NewRateLimitReader(ctx context.Context, reader Reader, bytesPerSecond uint64) Reader {
	if bytesPerSecond == 0 {
		return reader
	}
	limiter, _ := NewRateLimiter(bytesPerSecond)
	return NewRateLimitReaderWithLimiter(ctx, reader, limiter)
}

// NewRateLimiter 建峰值桶：burst 按 1/8 秒窗口推。
func NewRateLimiter(bytesPerSecond uint64) (*rate.Limiter, int) {
	return NewRateLimiterWithBurst(bytesPerSecond, 0)
}

// NewRateLimiterWithBurst 建桶并显式指定 burst（字节）。burstBytes = 0 时退回
// 1/8 秒窗口的默认策略。承诺桶（CIR）用这个入口，因为它的 burst 是业务给的
// CBS ——「能以峰值速率花掉多少」——而不是防锯齿的小窗口。
func NewRateLimiterWithBurst(bytesPerSecond, burstBytes uint64) (*rate.Limiter, int) {
	burst := rateLimitBurst(bytesPerSecond)
	if burstBytes > 0 {
		burst = clampBurst(burstBytes)
	}
	return rate.NewLimiter(rate.Limit(bytesPerSecond), burst), burst
}

// clampBurst 把字节数收进 int。32 位平台上 CBS（可能是几十 GB）放不下 int，
// 溢出会变成负 burst，让 WaitN 永远失败——那是「限速配大了反而断连」，
// 比限不住严重得多。所以宁可截断到本平台能表达的最大值。
func clampBurst(burstBytes uint64) int {
	if burstBytes > uint64(math.MaxInt) {
		return math.MaxInt
	}
	burst := int(burstBytes)
	if burst < Size {
		burst = Size
	}
	return burst
}

// NewRateLimitReaderWithLimiter wraps reader with shared token buckets.
// ctx 绑定连接生命周期：连接关闭后 WaitN 立即返回，不再睡在共享桶上占配额。
func NewRateLimitReaderWithLimiter(ctx context.Context, reader Reader, limiters ...*rate.Limiter) Reader {
	live := compactLimiters(limiters)
	if len(live) == 0 {
		return reader
	}
	return &RateLimitReader{Reader: reader, ctx: ctx, limiters: live}
}

func NewRateLimitWriterWithLimiter(ctx context.Context, writer Writer, limiters ...*rate.Limiter) Writer {
	live := compactLimiters(limiters)
	if len(live) == 0 {
		return writer
	}
	return &RateLimitWriter{Writer: writer, ctx: ctx, limiters: live}
}

// compactLimiters 丢掉 nil。调用方（dispatcher）可以无脑把「峰值桶, 承诺桶」
// 一起传进来，没配承诺速率时后者是 nil。
func compactLimiters(limiters []*rate.Limiter) []*rate.Limiter {
	live := limiters[:0:0]
	for _, l := range limiters {
		if l != nil {
			live = append(live, l)
		}
	}
	return live
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
	if waitErr := r.wait(int(mb.Len())); waitErr != nil && err == nil {
		return mb, waitErr
	}
	return mb, err
}

func (w *RateLimitWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if mb.IsEmpty() {
		return w.Writer.WriteMultiBuffer(mb)
	}
	if err := w.wait(int(mb.Len())); err != nil {
		return err
	}
	return w.Writer.WriteMultiBuffer(mb)
}

// rateLimitWaitN 让这一批字节**依次通过每一个桶**（串联整形），全部取到才放行。
//
// 串联而不是取最小速率，是因为两个桶的深度不同：峰值桶浅（1/8 秒窗口），承诺桶深
// （CBS，控制面给的额度）。新连接上来时承诺桶是满的，立刻放行，只有峰值桶在
// 排队 → 跑 PIR；CBS 花完后承诺桶开始按 CIR 滴令牌 → 自然落到 CIR。这正是双速率
// 要的行为，且不需要任何额外的状态机。
//
// 顺序等待不会算错速率：在第一个桶上等待的这段时间里，第二个桶一直在攒令牌，
// 所以稳态吞吐就是 min(各桶速率)。
func rateLimitWaitN(ctx context.Context, limiters []*rate.Limiter, total int) error {
	for _, limiter := range limiters {
		if err := rateLimitWaitOne(ctx, limiter, total); err != nil {
			return err
		}
	}
	return nil
}

// rateLimitWaitOne 按 limiter 当前 burst 分片阻塞取令牌。
//   - ctx 绑定连接生命周期：连接关闭后立即带错返回，不再睡在共享桶上占队。
//   - burst 每轮动态读：节点公平调度器会并发 SetBurst 调整窗口；若在 Burst() 与
//     WaitN 之间 burst 被调小，WaitN(n>burst) 会立即报错——此时 ctx 仍存活则按新
//     burst 重试（n 单调收缩、必有进展），保证只慢不断连。
//
// 分片按**单个桶自己的 burst** 来，不是全局取最小：每个桶各自被扣满 total 个令牌，
// 不会因为邻桶 burst 小而重复扣款。
func rateLimitWaitOne(ctx context.Context, limiter *rate.Limiter, total int) error {
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
