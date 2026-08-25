package outbound

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

func payload(size int) buf.MultiBuffer {
	var result buf.MultiBuffer
	for size > 0 {
		chunk := size
		if chunk > buf.Size {
			chunk = buf.Size
		}
		b := buf.New()
		b.Extend(int32(chunk))
		result = append(result, b)
		size -= chunk
	}
	return result
}

func wrappedRateLimitLink(t *testing.T, limiter *outboundRateLimiter) (*transport.Link, *buf.MultiBufferContainer) {
	t.Helper()
	sink := &buf.MultiBufferContainer{}
	link := &transport.Link{
		Reader: &buf.MultiBufferContainer{},
		Writer: sink,
	}
	limiter.WrapLink(context.Background(), link)
	t.Cleanup(func() { buf.ReleaseMulti(sink.MultiBuffer) })
	return link, sink
}

func TestOutboundRateLimitIsSharedAcrossOneHundredConnections(t *testing.T) {
	const (
		connectionCount = 100
		bytesPerClient  = 4 * 1024
		rateBitPerSec   = 8_000_000 // 1 MB/s, 125 KB initial burst
	)

	limited := newOutboundRateLimiter(rateBitPerSec)
	independent := newOutboundRateLimiter(0)

	links := make([]*transport.Link, 0, connectionCount)
	for range connectionCount {
		link, _ := wrappedRateLimitLink(t, limited)
		links = append(links, link)
	}
	independentLink, _ := wrappedRateLimitLink(t, independent)

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, connectionCount)
	for _, link := range links {
		wg.Add(1)
		go func(link *transport.Link) {
			defer wg.Done()
			errs <- link.Writer.WriteMultiBuffer(payload(bytesPerClient))
		}(link)
	}

	independentStart := time.Now()
	if err := independentLink.Writer.WriteMultiBuffer(payload(bytesPerClient)); err != nil {
		t.Fatalf("independent outbound write failed: %v", err)
	}
	if elapsed := time.Since(independentStart); elapsed > 100*time.Millisecond {
		t.Fatalf("another outbound was delayed by the limited outbound: %v", elapsed)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("limited outbound write failed: %v", err)
		}
	}
	elapsed := time.Since(start)
	// 409,600 B through one 1 MB/s bucket with a 125,000 B initial burst takes
	// about 285 ms. Per-connection buckets would finish almost immediately.
	if elapsed < 180*time.Millisecond {
		t.Fatalf("100 connections did not share one outbound bucket: %v", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("shared outbound limiter was unexpectedly slow: %v", elapsed)
	}
	t.Logf("100 connections moved %d bytes through one outbound in %v", connectionCount*bytesPerClient, elapsed)
}

func TestOutboundRateLimitHotUpdateAffectsExistingConnection(t *testing.T) {
	limiter := newOutboundRateLimiter(0)
	link, _ := wrappedRateLimitLink(t, limiter)

	if err := link.Writer.WriteMultiBuffer(payload(1)); err != nil {
		t.Fatalf("initial unlimited write failed: %v", err)
	}

	limiter.SetBitPerSec(800_000) // 100 KB/s, 12.5 KB burst
	start := time.Now()
	if err := link.Writer.WriteMultiBuffer(payload(50_000)); err != nil {
		t.Fatalf("existing connection broke after enabling the cap: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("existing connection ignored the hot rate update: %v", elapsed)
	}

	limiter.SetBitPerSec(0)
	start = time.Now()
	if err := link.Writer.WriteMultiBuffer(payload(50_000)); err != nil {
		t.Fatalf("existing connection broke after disabling the cap: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("existing connection stayed capped after disabling the limit: %v", elapsed)
	}
}

func TestOutboundRateLimitUsesBitsPerSecond(t *testing.T) {
	limiter := newOutboundRateLimiter(8_000_000)
	if got := limiter.limiter.Limit(); got != 1_000_000 {
		t.Fatalf("8,000,000 bit/s must become 1,000,000 byte/s, got %v", got)
	}
	if got := outboundRateLimitBurst(8_000_000); got != 125_000 {
		t.Fatalf("125 ms burst at 8 Mbit/s must be 125,000 bytes, got %d", got)
	}
}

type rateLimitRecordingProxy struct {
	forceBuffered bool
	readerWrapped bool
	writerWrapped bool
}

func (p *rateLimitRecordingProxy) Process(ctx context.Context, link *transport.Link, _ internet.Dialer) error {
	outbounds := session.OutboundsFromContext(ctx)
	p.forceBuffered = outbounds[len(outbounds)-1].ForceBufferedCopy
	_, p.readerWrapped = link.Reader.(*buf.RateLimitReader)
	_, p.writerWrapped = link.Writer.(*buf.RateLimitWriter)
	return nil
}

func TestManagedOutboundWrapsBothDirectionsAndDisablesSplice(t *testing.T) {
	proxy := &rateLimitRecordingProxy{}
	handler := &Handler{
		proxy:       proxy,
		rateLimiter: newOutboundRateLimiter(0),
	}
	link := &transport.Link{
		Reader: &buf.MultiBufferContainer{},
		Writer: &buf.MultiBufferContainer{},
	}
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{}})
	handler.Dispatch(ctx, link)

	if !proxy.readerWrapped || !proxy.writerWrapped {
		t.Fatalf("managed outbound wrappers: reader=%v writer=%v", proxy.readerWrapped, proxy.writerWrapped)
	}
	if !proxy.forceBuffered {
		t.Fatal("managed outbound must disable splice or the rate wrappers can be bypassed")
	}
}

func TestUnconfiguredOutboundRejectsHotRateLimit(t *testing.T) {
	handler := &Handler{}
	if err := handler.SetOutboundRateLimitBitPerSec(8_000_000); err == nil {
		t.Fatal("an outbound that omitted rate_limit_bit_per_sec cannot hot-limit existing connections")
	}
}
