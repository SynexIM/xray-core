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
	"github.com/xtls/xray-core/transport/pipe"
)

// TestSniffingCachedReaderWithRateLimitedOutbound is the regression guard for
// the production crash-loop: Dispatch's sniffing path used to do
// `outbound.Reader.(*pipe.Reader)` — an unguarded type assertion. Once a user
// has a rate limit, getLink wraps outboundLink.Reader in *buf.RateLimitReader,
// so the assertion panicked and took down the whole xray process
// ("interface conversion: buf.Reader is *buf.RateLimitReader, not *pipe.Reader").
func TestSniffingCachedReaderWithRateLimitedOutbound(t *testing.T) {
	d := &DefaultDispatcher{policy: policy.DefaultManager{}}
	user := &protocol.MemoryUser{Email: "sniff@ratelimit.test", BandwidthBps: 8_000_000}
	t.Cleanup(user.ResetRuntimeLimiter)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		User:   user,
		Source: net.TCPDestination(net.LocalHostIP, 12345),
	})
	in, out := d.getLink(ctx, net.Destination{})

	if _, ok := out.Reader.(*pipe.Reader); ok {
		t.Fatal("precondition failed: rate-limited outbound.Reader should be wrapped, got raw *pipe.Reader")
	}

	// 旧代码在这里强转 *pipe.Reader 直接 panic。
	cReader := &cachedReader{reader: asTimeoutReader(out.Reader)}

	payload := buf.New()
	payload.WriteString("hello")
	common.Must(in.Writer.WriteMultiBuffer(buf.MultiBuffer{payload}))
	mb, err := cReader.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatalf("read through cachedReader over rate-limited pipe failed: %v", err)
	}
	if mb.Len() != 5 {
		t.Fatalf("expected 5 bytes through cachedReader, got %d", mb.Len())
	}

	// Interrupt 必须穿透限速包装器打断底层 pipe，否则连接无法被中止。
	cReader.Interrupt()
	if _, err := cReader.ReadMultiBuffer(); err == nil {
		t.Fatal("expected read error after Interrupt: pipe was not interrupted through the rate-limit wrapper")
	}
}
