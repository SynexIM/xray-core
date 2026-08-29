package dispatcher

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
)

type egressTestHandler struct {
	outbound.Handler
	tag string
}

func (h *egressTestHandler) Tag() string                             { return h.tag }
func (*egressTestHandler) Dispatch(context.Context, *transport.Link) {}

type egressTestManager struct {
	outbound.Manager
	egress   outbound.Handler
	fallback outbound.Handler
}

func (m *egressTestManager) GetHandler(tag string) outbound.Handler {
	if m.egress != nil && m.egress.Tag() == tag {
		return m.egress
	}
	return nil
}

func (m *egressTestManager) GetDefaultHandler() outbound.Handler { return m.fallback }

type egressTestRouter struct {
	routing.Router
	calls int
}

func (r *egressTestRouter) PickRoute(routing.Context) (routing.Route, error) {
	r.calls++
	return nil, common.ErrNoClue
}

func egressTestContext(user *protocol.MemoryUser) (context.Context, *session.Outbound) {
	ob := new(session.Outbound)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{User: user})
	return session.ContextWithOutbounds(ctx, []*session.Outbound{ob}), ob
}

func TestEgressTagBypassesRouter(t *testing.T) {
	dedicated := &egressTestHandler{tag: "dedicated"}
	fallback := &egressTestHandler{tag: "fallback"}
	router := new(egressTestRouter)
	d := &DefaultDispatcher{
		ohm:    &egressTestManager{egress: dedicated, fallback: fallback},
		router: router,
	}
	ctx, ob := egressTestContext(&protocol.MemoryUser{EgressTag: "dedicated"})
	d.routedDispatch(ctx, &transport.Link{}, net.TCPDestination(net.LocalHostIP, 443))
	if router.calls != 0 {
		t.Fatalf("router was called %d times for a pinned user", router.calls)
	}
	if ob.Tag != "dedicated" {
		t.Fatalf("selected outbound = %q, want dedicated", ob.Tag)
	}
}

func TestEmptyEgressTagFallsBackToRouter(t *testing.T) {
	fallback := &egressTestHandler{tag: "fallback"}
	router := new(egressTestRouter)
	d := &DefaultDispatcher{
		ohm:    &egressTestManager{fallback: fallback},
		router: router,
	}
	ctx, ob := egressTestContext(&protocol.MemoryUser{})
	d.routedDispatch(ctx, &transport.Link{}, net.TCPDestination(net.LocalHostIP, 443))
	if router.calls != 1 {
		t.Fatalf("router was called %d times, want 1", router.calls)
	}
	if ob.Tag != "fallback" {
		t.Fatalf("selected outbound = %q, want fallback", ob.Tag)
	}
}
