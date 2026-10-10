package outbound

import (
	"context"
	"math"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport"
	"golang.org/x/time/rate"
)

// outboundRateLimiter owns one stable token bucket for one outbound handler.
// Reader and writer wrappers, across every connection, share this exact bucket,
// so the configured rate caps aggregate payload in both directions.
type outboundRateLimiter struct {
	limiter *rate.Limiter
}

func newOutboundRateLimiter(bitPerSec uint64) *outboundRateLimiter {
	if bitPerSec == 0 {
		return &outboundRateLimiter{
			// Keep one live limiter even while disabled. Existing connections can
			// then start observing a cap after a hot update without being re-wrapped.
			limiter: rate.NewLimiter(rate.Inf, buf.Size),
		}
	}
	return &outboundRateLimiter{
		limiter: rate.NewLimiter(rate.Limit(bitPerSec)/8, outboundRateLimitBurst(bitPerSec)),
	}
}

func (l *outboundRateLimiter) SetBitPerSec(bitPerSec uint64) {
	now := time.Now()
	if bitPerSec == 0 {
		l.limiter.SetLimitAt(now, rate.Inf)
		return
	}

	newLimit := rate.Limit(bitPerSec) / 8
	newBurst := outboundRateLimitBurst(bitPerSec)
	if newLimit < l.limiter.Limit() {
		// Tighten the rate before shrinking the burst so a downward update
		// never has a window at the old, faster rate.
		l.limiter.SetLimitAt(now, newLimit)
		l.limiter.SetBurstAt(now, newBurst)
		return
	}
	l.limiter.SetBurstAt(now, newBurst)
	l.limiter.SetLimitAt(now, newLimit)
}

func (l *outboundRateLimiter) WrapLink(ctx context.Context, link *transport.Link) {
	link.Reader = buf.NewRateLimitReaderWithLimiter(ctx, link.Reader, l.limiter)
	link.Writer = buf.NewRateLimitWriterWithLimiter(ctx, link.Writer, l.limiter)
}

// One token is one byte. The configured public unit is bit/s, and the burst is
// the same 125 ms window used by the existing payload limiters.
func outboundRateLimitBurst(bitPerSec uint64) int {
	burstBytes := bitPerSec / 64
	if bitPerSec%64 != 0 {
		burstBytes++
	}
	if burstBytes < uint64(buf.Size) {
		return buf.Size
	}
	if burstBytes > uint64(math.MaxInt) {
		return math.MaxInt
	}
	return int(burstBytes)
}
