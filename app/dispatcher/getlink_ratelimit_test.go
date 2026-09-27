package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
)

// pumpAndTime writes `total` bytes into w (in a goroutine) and drains them from
// r on the calling goroutine, returning how long draining took. The rate limiter
// wraps the READ side of each getLink pipe, so the reader is what gets throttled.
func pumpAndTime(t *testing.T, w buf.Writer, r buf.Reader, total int) time.Duration {
	t.Helper()
	writeErr := make(chan error, 1)
	go func() {
		remaining := total
		for remaining > 0 {
			n := remaining
			if n > buf.Size {
				n = buf.Size
			}
			b := buf.New()
			b.Extend(int32(n))
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				writeErr <- err
				return
			}
			remaining -= n
		}
		_ = common.Close(w)
		writeErr <- nil
	}()

	start := time.Now()
	read := 0
	for read < total {
		mb, err := r.ReadMultiBuffer()
		read += int(mb.Len())
		buf.ReleaseMulti(mb)
		if err != nil {
			break
		}
	}
	elapsed := time.Since(start)
	if err := <-writeErr; err != nil {
		t.Fatalf("write pump failed: %v", err)
	}
	if read < total {
		t.Fatalf("only drained %d of %d bytes", read, total)
	}
	return elapsed
}

// TestGetLinkRateLimitsBothUplinkAndDownlink is the regression guard for the
// upload-not-capped bug: the per-user token bucket must throttle BOTH pipes that
// getLink builds, not just the downlink one.
//
// getLink's topology is two independent pipes (unlike WrapLink's single
// bidirectional link):
//
//	downlink pipe = inboundLink.Reader (read) / outboundLink.Writer (write)
//	uplink   pipe = inboundLink.Writer (write) / outboundLink.Reader (read)
//
// The old code wrapped inboundLink.Reader AND outboundLink.Writer — i.e. both
// ends of the DOWNLINK pipe — leaving the uplink pipe completely unshaped, so
// upload was unlimited. This test drives each pipe end-to-end with real
// pipe.New pipes and the real per-user rate.Limiter and asserts BOTH directions
// are measurably throttled. It exercises the exact getLink path the plain-HTTP
// (PROXY_PROTOCOL_MIXED) inbound drives via Dispatch; the shaping logic under
// test lives entirely in getLink.
func TestGetLinkRateLimitsBothUplinkAndDownlink(t *testing.T) {
	// 1 MB/s cap. burst == rate == 1_000_000 bytes, so the first 1 MB is free and
	// the next 0.5 MB is throttled at 1 MB/s => ~0.5s of enforced delay for a
	// 1.5 MB transfer. An UNLIMITED direction drains 1.5 MB in a few ms.
	const (
		ratePerSecBytes = 1_000_000
		bandwidthBps    = uint64(ratePerSecBytes * 8)
		transferBytes   = 1_500_000
		// Well above the "unlimited" baseline (~ms) and safely below the ~0.5s
		// theoretical throttle floor, tolerant of scheduler jitter on slow CI.
		minThrottle = 300 * time.Millisecond
	)

	d := &DefaultDispatcher{policy: policy.DefaultManager{}}

	newLinks := func(email string) (inboundLink, outboundLink *transport.Link) {
		user := &protocol.MemoryUser{Email: email, BandwidthBps: bandwidthBps}
		t.Cleanup(user.ResetRuntimeLimiter)
		ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
			User:   user,
			Source: net.TCPDestination(net.LocalHostIP, 12345),
		})
		return d.getLink(ctx, net.Destination{})
	}

	t.Run("uplink is capped", func(t *testing.T) {
		// Fresh user => full bucket, isolated from the downlink subtest.
		in, out := newLinks("uplink@ratelimit.test")
		// uplink pipe: write to inboundLink.Writer, read from outboundLink.Reader.
		elapsed := pumpAndTime(t, in.Writer, out.Reader, transferBytes)
		if elapsed < minThrottle {
			t.Fatalf("uplink drained %d bytes in %v (< %v) — upload is NOT rate limited (regression)", transferBytes, elapsed, minThrottle)
		}
	})

	t.Run("downlink is capped", func(t *testing.T) {
		in, out := newLinks("downlink@ratelimit.test")
		// downlink pipe: write to outboundLink.Writer, read from inboundLink.Reader.
		elapsed := pumpAndTime(t, out.Writer, in.Reader, transferBytes)
		if elapsed < minThrottle {
			t.Fatalf("downlink drained %d bytes in %v (< %v) — download is NOT rate limited (regression)", transferBytes, elapsed, minThrottle)
		}
	})
}
