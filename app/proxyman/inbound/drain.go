package inbound

import (
	"errors"
	"net"

	"github.com/xtls/xray-core/transport/internet"
)

var errDrainUnsupported = errors.New("listener transport cannot preserve existing sessions during drain")
var errNotRunning = errors.New("inbound is not running")

type drainingWorker interface {
	CanDrain() error
	Drain() error
	Resume() error
}

// Preflight all workers before touching any listener. In particular, do not
// call Close on a gRPC/H3 server: that closes established application streams.
func (h *AlwaysOnInboundHandler) Drain() error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if !h.running || h.closed {
		return errNotRunning
	}
	if len(h.workers) == 0 {
		return errDrainUnsupported
	}
	for _, w := range h.workers {
		d, ok := w.(drainingWorker)
		if !ok {
			return errDrainUnsupported
		}
		if err := d.CanDrain(); err != nil {
			return err
		}
	}
	var errs []error
	for _, w := range h.workers {
		if err := w.(drainingWorker).Drain(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *AlwaysOnInboundHandler) Resume() error {
	h.lifecycle.Lock()
	defer h.lifecycle.Unlock()
	if !h.running || h.closed {
		return errNotRunning
	}
	var errs []error
	for _, w := range h.workers {
		d, ok := w.(drainingWorker)
		if !ok {
			return errDrainUnsupported
		}
		if err := d.Resume(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Stream servers must retain ownership of old streams after releasing their
// listening address. Plain TCP/WebSocket listeners do not own accepted streams.
// Multiplexed logical sessions may span HTTP requests/connections. Their
// owner pauses admission and retains the listening socket until final Close.
type pausableListener interface {
	internet.Listener
	Drain() error
	Resume() error
}

type drainableListener interface {
	internet.Listener
	Drain() error
	Drained() bool
}

func pruneDrained(hubs []drainableListener) []drainableListener {
	live := hubs[:0]
	for _, hub := range hubs {
		if !hub.Drained() {
			live = append(live, hub)
		}
	}
	clear(hubs[len(live):])
	return live
}

func drainStream(hub internet.Listener, retired []drainableListener) ([]drainableListener, error) {
	retired = pruneDrained(retired)
	if d, ok := hub.(drainableListener); ok {
		if err := d.Drain(); err != nil {
			return retired, err
		}
		return append(retired, d), nil
	}
	err := hub.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	return retired, err
}

func canDrainStream(stream *internet.MemoryStreamConfig) error {
	if stream == nil {
		return nil
	}
	switch stream.ProtocolName {
	case "tcp", "websocket":
		return nil
	default:
		return errDrainUnsupported
	}
}

func (w *tcpWorker) CanDrain() error {
	if w.drained {
		return nil
	}
	if _, ok := w.hub.(pausableListener); ok {
		return nil
	}
	if _, ok := w.hub.(drainableListener); ok {
		return nil
	}
	return canDrainStream(w.stream)
}
func (w *tcpWorker) Drain() error {
	if w.drained {
		return nil
	}
	if err := w.CanDrain(); err != nil {
		return err
	}
	if w.hub == nil {
		return errNotRunning
	}
	if p, ok := w.hub.(pausableListener); ok {
		if err := p.Drain(); err != nil {
			return err
		}
		w.drained = true
		return nil
	}
	retired, err := drainStream(w.hub, w.retiredHubs)
	w.retiredHubs = retired
	if err != nil {
		return err
	}
	w.hub = nil
	w.drained = true
	return nil
}
func (w *tcpWorker) Resume() error {
	w.retiredHubs = pruneDrained(w.retiredHubs)
	if !w.drained {
		return nil
	}
	if p, ok := w.hub.(pausableListener); ok {
		if err := p.Resume(); err != nil {
			return err
		}
	} else if err := w.Start(); err != nil {
		return err
	}
	w.drained = false
	return nil
}

func (w *dsWorker) CanDrain() error {
	if w.drained {
		return nil
	}
	if _, ok := w.hub.(pausableListener); ok {
		return nil
	}
	if _, ok := w.hub.(drainableListener); ok {
		return nil
	}
	return canDrainStream(w.stream)
}
func (w *dsWorker) Drain() error {
	if w.drained {
		return nil
	}
	if err := w.CanDrain(); err != nil {
		return err
	}
	if w.hub == nil {
		return errNotRunning
	}
	if p, ok := w.hub.(pausableListener); ok {
		if err := p.Drain(); err != nil {
			return err
		}
		w.drained = true
		return nil
	}
	retired, err := drainStream(w.hub, w.retiredHubs)
	w.retiredHubs = retired
	if err != nil {
		return err
	}
	w.hub = nil
	w.drained = true
	return nil
}
func (w *dsWorker) Resume() error {
	w.retiredHubs = pruneDrained(w.retiredHubs)
	if !w.drained {
		return nil
	}
	if p, ok := w.hub.(pausableListener); ok {
		if err := p.Resume(); err != nil {
			return err
		}
	} else if err := w.Start(); err != nil {
		return err
	}
	w.drained = false
	return nil
}

// UDP associations share their socket. Retain it for existing associations and
// reject new source/destination keys until resume; never recreate activeConn.
func (w *udpWorker) CanDrain() error { return nil }
func (w *udpWorker) Drain() error    { w.Lock(); defer w.Unlock(); w.draining = true; return nil }
func (w *udpWorker) Resume() error   { w.Lock(); defer w.Unlock(); w.draining = false; return nil }
