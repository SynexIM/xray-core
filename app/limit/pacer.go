package limit

import (
	"container/heap"
	"context"
	"math"
	"runtime"
	"slices"
	"sync/atomic"
	"time"
)

type flowRate struct {
	flow    *Flow
	rate    float64
	version uint64
}

var packetSequence atomic.Uint64

func newConnectionPacer(flows []*Flow, activity func() bool, meter, participating *atomic.Uint64, datagram bool) *connectionPacer {
	pacer := &connectionPacer{flows: flows, activity: activity, meter: meter, participatingMeter: participating, datagram: datagram}
	pacer.initialize()
	return pacer
}
func (pacer *connectionPacer) initialize() {
	if pacer.initialized {
		return
	}
	pacer.initialized = true
	if len(pacer.flows) <= 1 {
		return
	}
	slices.SortFunc(pacer.flows, func(first, second *Flow) int {
		if first.s.number < second.s.number {
			return -1
		}
		if first.s.number > second.s.number {
			return 1
		}
		return 0
	})
	pacer.rates = make([]flowRate, len(pacer.flows))
	for index, flow := range pacer.flows {
		pacer.rates[index].flow = flow
	}
	pacer.notify = make(chan struct{}, 1)
	pacer.timer = time.NewTimer(time.Hour)
	pacer.timer.Stop()
	pacer.refreshRates()
}
func (pacer *connectionPacer) refreshRates() {
	changed := false
	for index := range pacer.rates {
		entry := &pacer.rates[index]
		rate, version := entry.flow.s.rateVersion()
		if entry.rate != rate || entry.version != version {
			changed = true
			entry.rate = rate
			entry.version = version
		}
	}
	if changed {
		slices.SortFunc(pacer.rates, func(first, second flowRate) int {
			if first.rate < second.rate {
				return -1
			}
			if first.rate > second.rate {
				return 1
			}
			return 0
		})
	}
}
func (pacer *connectionPacer) close() {
	pacer.closed.Store(true)
	if pacer.timer != nil {
		pacer.timer.Stop()
	}
	pacer.wake()
}
func (pacer *connectionPacer) wake() {
	select {
	case pacer.notify <- struct{}{}:
	default:
	}
}

func (pacer *connectionPacer) Wait(ctx context.Context, n int, datagram bool) error {
	pacer.initialize()
	pacer.refreshRates()
	var err error
	if len(pacer.flows) == 1 {
		err = pacer.flows[0].wait(ctx, n, datagram)
	} else if len(pacer.flows) > 1 {
		err = pacer.waitTogether(ctx, n, datagram)
	}
	if err != nil {
		return err
	}
	if pacer.socket != nil {
		rate := math.Inf(1)
		for _, flow := range pacer.flows {
			rate = min(rate, flow.s.pacingRate(flow))
		}
		return pacer.socket.update(rate)
	}
	return nil
}
func (pacer *connectionPacer) lockFlows() {
	for _, flow := range pacer.flows {
		flow.s.mu.Lock()
	}
}
func (pacer *connectionPacer) unlockFlows() {
	for index := len(pacer.flows) - 1; index >= 0; index-- {
		pacer.flows[index].s.mu.Unlock()
	}
}

func (pacer *connectionPacer) waitTogether(ctx context.Context, n int, datagram bool) error {
	for n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		pacer.lockFlows()
		chunk := n
		for _, flow := range pacer.flows {
			flow.s.advance(time.Now())
			if !datagram || float64(n) > 2*flow.s.quantumBytes() {
				chunk = min(chunk, flow.s.sliceBytes())
			}
		}
		sparse := true
		var start uint64
		for _, flow := range pacer.flows {
			flow.s.sweep(time.Now())
			flow.s.meter(flow, time.Now())
			sparse = sparse && (datagram || flow.s.sparse(flow, chunk))
			start = max(start, max(flow.s.vtime, flow.finish))
		}
		sequence := packetSequence.Add(1)
		for _, flow := range pacer.flows {
			shaper := flow.s
			waiting := &flow.waiter
			*waiting = waiter{flow: flow, sparse: sparse, seq: sequence, n: chunk, index: -1, group: pacer}
			if !waiting.sparse {
				waiting.start = start
				flow.finish = waiting.start + uint64(chunk)
			}
			heap.Push(&shaper.queue, waiting)
			flow.waiting = true
			shaper.active[flow] = struct{}{}
		}
		pacer.unlockFlows()
		if err := pacer.awaitTogether(ctx); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}
func (pacer *connectionPacer) clearWaitersLocked() {
	for _, flow := range pacer.flows {
		waiting := &flow.waiter
		if waiting.index >= 0 {
			heap.Remove(&flow.s.queue, waiting.index)
		}
		flow.waiting = false
		flow.s.dispatchLocked()
	}
}
func (pacer *connectionPacer) awaitTogether(ctx context.Context) error {
	for {
		pacer.lockFlows()
		failure := ctx.Err()
		ready := true
		var delay time.Duration
		for _, flow := range pacer.flows {
			shaper := flow.s
			shaper.advance(time.Now())
			if shaper.closed || pacer.closed.Load() {
				failure = errShaperClosed
			}
			waiting := &flow.waiter
			if waiting.err != nil {
				failure = waiting.err
			}
			unlimited := shaper.rate == 0 && !shaper.congested
			atHead := shaper.queue.Len() > 0 && shaper.queue[0] == waiting
			if !atHead || (!unlimited && !shaper.ready(waiting)) {
				ready = false
			}
			if !atHead {
				shaper.dispatchLocked()
				delay = max(delay, time.Millisecond)
			} else if !unlimited {
				delay = max(delay, shaper.waitDelayLocked(waiting))
			}
		}
		if failure != nil {
			pacer.clearWaitersLocked()
			pacer.unlockFlows()
			return failure
		}
		if ready {
			for _, flow := range pacer.flows {
				waiting := &flow.waiter
				heap.Remove(&flow.s.queue, waiting.index)
				flow.waiting = false
				flow.s.grantLocked(flow, waiting.n)
				if !waiting.sparse {
					flow.s.vtime = max(flow.s.vtime, waiting.start)
				}
			}
			for _, flow := range pacer.flows {
				flow.s.dispatchLocked()
			}
			pacer.unlockFlows()
			return nil
		}
		for _, flow := range pacer.flows {
			flow.s.waits++
		}
		pacer.unlockFlows()
		if delay < time.Millisecond {
			runtime.Gosched()
			continue
		}
		pacer.timer.Reset(delay)
		select {
		case <-ctx.Done():
		case <-pacer.notify:
		case <-pacer.timer.C:
		}
		pacer.timer.Stop()
	}
}
