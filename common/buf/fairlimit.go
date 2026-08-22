package buf

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// fairBlockedThreshold 是「算作阻塞」的等待时长下限。
//
// 为什么需要一个下限：WaitN 即使不需要排队也会花掉几微秒（取锁、算令牌）。
// 把那个也算成阻塞，所有人都会永远是 backlogged，需求测量就失去意义了。
// 1ms 在 8KB 缓冲下对应约 8MB/s —— 真正跑满桶的人每读几次就会撞上一次远超 1ms
// 的等待，而用量远低于额度的人一次也撞不上。
const fairBlockedThreshold = time.Millisecond

// FairLimitReader 是节点级公平限速的上行（Reader）包装器。
// 与 RateLimitReader 同心智（按读到字节 WaitN 阻塞整形），但额外回调两件事：
//
//	onBytes    经过多少字节 —— 活跃判定 + satisfied 成员的需求测量
//	onBlocked  这次真的等了令牌 —— backlogged 信号（FR-074）
//
// 调度器就靠这两个信号分 satisfied / backlogged，不猜数值、不做 DPI。
// limiter 的速率/burst 由 NodeFairScheduler 后台周期重算，本包不持有调度逻辑。
// ctx 绑定连接生命周期：连接关闭后不再睡在共享公平桶上占配额。
type FairLimitReader struct {
	Reader
	ctx       context.Context
	limiter   *rate.Limiter
	onBytes   func(int)
	onBlocked func(time.Duration, int)
}

func NewFairLimitReader(ctx context.Context, reader Reader, limiter *rate.Limiter, onBytes func(int), onBlocked func(time.Duration, int)) Reader {
	if limiter == nil {
		return reader
	}
	return &FairLimitReader{Reader: reader, ctx: ctx, limiter: limiter, onBytes: onBytes, onBlocked: onBlocked}
}

func (r *FairLimitReader) ReadMultiBuffer() (MultiBuffer, error) {
	return r.readWithTimeout(0, false)
}

func (r *FairLimitReader) ReadMultiBufferTimeout(timeout time.Duration) (MultiBuffer, error) {
	return r.readWithTimeout(timeout, true)
}

func (r *FairLimitReader) readWithTimeout(timeout time.Duration, withTimeout bool) (MultiBuffer, error) {
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
	n := int(mb.Len())
	if r.onBytes != nil {
		r.onBytes(n)
	}
	if waitErr := fairWait(r.ctx, r.limiter, n, r.onBlocked); waitErr != nil && err == nil {
		return mb, waitErr
	}
	return mb, err
}

// FairLimitWriter 是节点级公平限速的下行（Writer）包装器。
// per-user 限速只在 uplink(Reader)；节点公平要按【双向合计】算总出口，故下行也要整形。
type FairLimitWriter struct {
	Writer
	ctx       context.Context
	limiter   *rate.Limiter
	onBytes   func(int)
	onBlocked func(time.Duration, int)
}

func NewFairLimitWriter(ctx context.Context, writer Writer, limiter *rate.Limiter, onBytes func(int), onBlocked func(time.Duration, int)) Writer {
	if limiter == nil {
		return writer
	}
	return &FairLimitWriter{Writer: writer, ctx: ctx, limiter: limiter, onBytes: onBytes, onBlocked: onBlocked}
}

func (w *FairLimitWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if mb.IsEmpty() {
		return w.Writer.WriteMultiBuffer(mb)
	}
	n := int(mb.Len())
	if w.onBytes != nil {
		w.onBytes(n)
	}
	// 写前按速率整形（阻塞到 token 够或连接 ctx 取消），再交付下游。
	_ = fairWait(w.ctx, w.limiter, n, w.onBlocked)
	return w.Writer.WriteMultiBuffer(mb)
}

// fairWait 取令牌并把「确实等过」报回调度器。
//
// 两次 time.Now 的代价：调用频率上限是 字节数 / 8KB，500 Mbps 的节点约 8000 次/秒，
// 合计不到 1ms/秒，且只在节点公平开启时付。用这个换掉「猜需求」是划算的。
func fairWait(ctx context.Context, limiter *rate.Limiter, n int, onBlocked func(time.Duration, int)) error {
	if onBlocked == nil {
		return rateLimitWaitOne(ctx, limiter, n)
	}
	start := time.Now()
	err := rateLimitWaitOne(ctx, limiter, n)
	if waited := time.Since(start); waited >= fairBlockedThreshold {
		onBlocked(waited, n)
	}
	return err
}
