package limit

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
)

type connectionPacer struct {
	flows              []*Flow
	activity           func() bool
	meter              *atomic.Uint64
	participatingMeter *atomic.Uint64
	datagram           bool
	rates              []flowRate
	initialized        bool
	notify             chan struct{}
	timer              *time.Timer
	closed             atomic.Bool
	socket             *socketPacer
}

func (p *connectionPacer) record(n int) {
	if n == 0 {
		return
	}
	p.meter.Add(uint64(n))
	if p.activity() && p.participatingMeter != nil {
		p.participatingMeter.Add(uint64(n))
	}
}
func (p *connectionPacer) split(mb buf.MultiBuffer) (buf.MultiBuffer, buf.MultiBuffer) {
	if len(p.flows) == 0 {
		return nil, mb
	}
	// UDP/XUDP 每个 Buffer 是一份报文，必须保留边界和目的地址。
	if p.datagram || mb[0].UDP != nil {
		remaining, first := buf.SplitFirst(mb)
		return remaining, buf.MultiBuffer{first}
	}
	p.initialize()
	rate := p.flows[0].s.currentRate()
	if len(p.rates) > 0 {
		p.refreshRates()
		rate = p.rates[0].rate
	}
	size := int32(buf.Size)
	if !math.IsInf(rate, 1) {
		size = int32(min(float64(buf.Size), max(float64(shaperMinSliceByte), rate*shaperSlice.Seconds())))
	}
	return buf.SplitSize(mb, size)
}

type pacedReader struct {
	buf.Reader
	ctx     context.Context
	cancel  context.CancelFunc
	pacer   *connectionPacer
	pending buf.MultiBuffer
	readErr error
}

func newPacedReader(ctx context.Context, reader buf.Reader, pacer *connectionPacer) *pacedReader {
	managed, cancel := context.WithCancel(ctx)
	return &pacedReader{Reader: reader, ctx: managed, cancel: cancel, pacer: pacer}
}
func (r *pacedReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return r.read(0, false) }
func (r *pacedReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	return r.read(timeout, true)
}
func (r *pacedReader) read(timeout time.Duration, timed bool) (buf.MultiBuffer, error) {
	if r.pending.IsEmpty() {
		if timed {
			tr, ok := r.Reader.(buf.TimeoutReader)
			if !ok {
				return nil, buf.ErrNotTimeoutReader
			}
			r.pending, r.readErr = tr.ReadMultiBufferTimeout(timeout)
		} else {
			r.pending, r.readErr = r.Reader.ReadMultiBuffer()
		}
	}
	if r.pending.IsEmpty() {
		return r.pending, r.readErr
	}
	remaining, chunk := r.pacer.split(r.pending)
	r.pending = remaining
	if err := r.pacer.Wait(r.ctx, int(chunk.Len()), r.pacer.datagram || chunk[0].UDP != nil); err != nil {
		buf.ReleaseMulti(chunk)
		r.pending = buf.ReleaseMulti(r.pending)
		return nil, err
	}
	r.pacer.record(int(chunk.Len()))
	if r.pending.IsEmpty() {
		return chunk, r.readErr
	}
	return chunk, nil
}
func (r *pacedReader) Interrupt()   { r.cancel(); common.Interrupt(r.Reader) }
func (r *pacedReader) Close() error { r.cancel(); return common.Close(r.Reader) }
func (r *pacedReader) ReturnAnError(err error) {
	if p, ok := r.Reader.(interface{ ReturnAnError(error) }); ok {
		p.ReturnAnError(err)
	}
}
func (r *pacedReader) Recover() error {
	if p, ok := r.Reader.(interface{ Recover() error }); ok {
		return p.Recover()
	}
	return nil
}

type pacedWriter struct {
	buf.Writer
	ctx    context.Context
	cancel context.CancelFunc
	pacer  *connectionPacer
}

func newPacedWriter(ctx context.Context, writer buf.Writer, pacer *connectionPacer) *pacedWriter {
	managed, cancel := context.WithCancel(ctx)
	return &pacedWriter{Writer: writer, ctx: managed, cancel: cancel, pacer: pacer}
}
func (w *pacedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if mb.IsEmpty() {
		return w.Writer.WriteMultiBuffer(mb)
	}
	for !mb.IsEmpty() {
		remaining, chunk := w.pacer.split(mb)
		mb = remaining
		n := int(chunk.Len())
		if err := w.pacer.Wait(w.ctx, n, w.pacer.datagram || chunk[0].UDP != nil); err != nil {
			buf.ReleaseMulti(chunk)
			buf.ReleaseMulti(mb)
			return err
		}
		if err := w.Writer.WriteMultiBuffer(chunk); err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
		w.pacer.record(n)
	}
	return nil
}
func (w *pacedWriter) Interrupt()   { w.cancel(); common.Interrupt(w.Writer) }
func (w *pacedWriter) Close() error { w.cancel(); return common.Close(w.Writer) }
