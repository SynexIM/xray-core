package dispatcher

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

// captureHook 注册一个同步收集器，返回收到的事件切片与清理函数。
func captureHook(t *testing.T) *[]AccessEvent {
	t.Helper()
	var got []AccessEvent
	RegisterAccessHook(func(ev AccessEvent) { got = append(got, ev) })
	t.Cleanup(func() { accessHook = nil })
	return &got
}

func ctxWith(email string, content *session.Content, ob *session.Outbound) context.Context {
	ctx := context.Background()
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: &protocol.MemoryUser{Email: email}})
	if content != nil {
		ctx = session.ContextWithContent(ctx, content)
	}
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{ob})
	return ctx
}

// Issue 1 核心断言：客户端送域名、freedom 拨通后回填 DialedRemoteAddr →
// AccessEvent 同时带 Domain 与 DestIP（IP 不再为空）。
func TestEmitAccessForOutbound_DomainWithResolvedIP(t *testing.T) {
	got := captureHook(t)
	ob := &session.Outbound{
		OriginalTarget:   net.TCPDestination(net.DomainAddress("github.com"), 443),
		Target:           net.TCPDestination(net.DomainAddress("github.com"), 443),
		DialedRemoteAddr: net.IPAddress([]byte{140, 82, 121, 4}),
	}
	emitAccessForOutbound(ctxWith("u1@ipipx", &session.Content{Protocol: "tls"}, ob), ob, 100)

	if len(*got) != 1 {
		t.Fatalf("want 1 event, got %d", len(*got))
	}
	ev := (*got)[0]
	if ev.Domain != "github.com" {
		t.Errorf("domain = %q, want github.com", ev.Domain)
	}
	if ev.DestIP != "140.82.121.4" {
		t.Errorf("destIP = %q, want 140.82.121.4 (IP must not be empty)", ev.DestIP)
	}
	if ev.Email != "u1@ipipx" {
		t.Errorf("email = %q", ev.Email)
	}
	if ev.Protocol != "tls" {
		t.Errorf("protocol = %q, want tls", ev.Protocol)
	}
}

// 客户端直接送 IP（无 SNI）：Domain 空、DestIP 取原始/拨通 IP。
func TestEmitAccessForOutbound_IPOnly(t *testing.T) {
	got := captureHook(t)
	ob := &session.Outbound{
		OriginalTarget:   net.TCPDestination(net.IPAddress([]byte{1, 1, 1, 1}), 443),
		Target:           net.TCPDestination(net.IPAddress([]byte{1, 1, 1, 1}), 443),
		DialedRemoteAddr: net.IPAddress([]byte{1, 1, 1, 1}),
	}
	emitAccessForOutbound(ctxWith("u2@ipipx", &session.Content{}, ob), ob, 200)

	if len(*got) != 1 {
		t.Fatalf("want 1 event, got %d", len(*got))
	}
	ev := (*got)[0]
	if ev.Domain != "" {
		t.Errorf("domain = %q, want empty (IP-only)", ev.Domain)
	}
	if ev.DestIP != "1.1.1.1" {
		t.Errorf("destIP = %q, want 1.1.1.1", ev.DestIP)
	}
}

// blackhole 禁陆：未拨通（DialedRemoteAddr nil），域名有、IP 空 —— 不造假 IP。
func TestEmitAccessForOutbound_BlockedNoDial(t *testing.T) {
	got := captureHook(t)
	ob := &session.Outbound{
		OriginalTarget: net.TCPDestination(net.DomainAddress("blocked.cn"), 443),
		Target:         net.TCPDestination(net.DomainAddress("blocked.cn"), 443),
		// DialedRemoteAddr nil：blackhole 从不拨通。
	}
	emitAccessForOutbound(ctxWith("u3@ipipx", &session.Content{Protocol: "tls"}, ob), ob, 300)

	if len(*got) != 1 {
		t.Fatalf("want 1 event, got %d", len(*got))
	}
	ev := (*got)[0]
	if ev.Domain != "blocked.cn" {
		t.Errorf("domain = %q, want blocked.cn", ev.Domain)
	}
	if ev.DestIP != "" {
		t.Errorf("destIP = %q, want empty (never dialed, no fake IP)", ev.DestIP)
	}
}

// hook 未注册：零开销，不 panic。
func TestEmitAccessForOutbound_NoHook(t *testing.T) {
	accessHook = nil
	ob := &session.Outbound{Target: net.TCPDestination(net.DomainAddress("x.com"), 443)}
	emitAccessForOutbound(ctxWith("u@x", nil, ob), ob, 1)
}
