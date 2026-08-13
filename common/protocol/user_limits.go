package protocol

import (
	"sync"

	"golang.org/x/time/rate"
)

type runtimeLimiterState struct {
	mu      sync.Mutex
	bps     uint64
	limiter *rate.Limiter
	burst   int
}

var runtimeLimiters sync.Map

// RuntimeLimits returns the LayerX per-user runtime limits carried in memory.
// Zero values mean unlimited and preserve upstream behavior.
func (u *MemoryUser) RuntimeLimits() (bandwidthBps uint64, connLimit uint32) {
	if u == nil {
		return 0, 0
	}
	return u.BandwidthBps, u.ConnLimit
}

// RuntimeRateLimiter returns the user's shared token bucket. All concurrent
// links for the same MemoryUser consume from this bucket. BandwidthBps is the
// wire/domain unit (bit/s); xray's rate limiter consumes bytes/s, so conversion
// is centralized here.
func (u *MemoryUser) RuntimeRateLimiter(newLimiter func(uint64) (*rate.Limiter, int)) (*rate.Limiter, int) {
	if u == nil || u.BandwidthBps == 0 {
		return nil, 0
	}
	// Fast path: avoid allocating a fresh state on every connection. LoadOrStore
	// eagerly evaluates its value argument, so a plain Load hit skips the alloc.
	raw, ok := runtimeLimiters.Load(u)
	if !ok {
		raw, _ = runtimeLimiters.LoadOrStore(u, new(runtimeLimiterState))
	}
	state := raw.(*runtimeLimiterState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.limiter == nil || state.bps != u.BandwidthBps {
		state.limiter, state.burst = newLimiter(bitsPerSecondToRuntimeBytesPerSecond(u.BandwidthBps))
		state.bps = u.BandwidthBps
	}
	return state.limiter, state.burst
}

func bitsPerSecondToRuntimeBytesPerSecond(bitsPerSecond uint64) uint64 {
	if bitsPerSecond == 0 {
		return 0
	}
	return (bitsPerSecond + 7) / 8
}

func (u *MemoryUser) ResetRuntimeLimiter() {
	if u == nil {
		return
	}
	runtimeLimiters.Delete(u)
}
