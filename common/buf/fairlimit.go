package buf

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// FairLimitReader 是节点级公平限速的上行（Reader）包装器（ipipx 魔改）。
// 与 RateLimitReader 同心智（按读到字节 WaitN 阻塞整形），但额外通过 onBytes 回调累加
// 经过字节供活跃判定。limiter 的速率/burst 由 NodeFairScheduler 后台周期重算
// （SetLimit/SetBurst），本包不持有调度逻辑——这里只做「按当前速率整形 + 计字节」。
// ctx 绑定连接生命周期：连接关闭后不再睡在共享公平桶上占配额。
//
// limiter 为 nil（节点公平未开启）时调用方不应构造本 wrapper（零开销）。
type FairLimitReader struct {
	Reader
	ctx     context.Context
	limiter *rate.Limiter
	onBytes func(int)
}

func NewFairLimitReader(ctx context.Context, reader Reader, limiter *rate.Limiter, onBytes func(int)) Reader {
	if limiter == nil {
		return reader
	}
	return &FairLimitReader{Reader: reader, ctx: ctx, limiter: limiter, onBytes: onBytes}
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
	if waitErr := rateLimitWaitN(r.ctx, r.limiter, n); waitErr != nil && err == nil {
		return mb, waitErr
	}
	return mb, err
}

// FairLimitWriter 是节点级公平限速的下行（Writer）包装器（ipipx 魔改）。
// per-user 限速只在 uplink(Reader)；节点公平要按【双向合计】算总出口，故下行也要整形。
// 写前按将写字节 WaitN 阻塞整形，并通过 onBytes 计字节。
type FairLimitWriter struct {
	Writer
	ctx     context.Context
	limiter *rate.Limiter
	onBytes func(int)
}

func NewFairLimitWriter(ctx context.Context, writer Writer, limiter *rate.Limiter, onBytes func(int)) Writer {
	if limiter == nil {
		return writer
	}
	return &FairLimitWriter{Writer: writer, ctx: ctx, limiter: limiter, onBytes: onBytes}
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
	_ = rateLimitWaitN(w.ctx, w.limiter, n)
	return w.Writer.WriteMultiBuffer(mb)
}
