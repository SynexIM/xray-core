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
	route routing.Route
}

func (r *egressTestRouter) PickRoute(routing.Context) (routing.Route, error) {
	r.calls++
	if r.route != nil {
		return r.route, nil
	}
	return nil, common.ErrNoClue
}

type egressTestRoute struct {
	routing.Route
	outTag, ruleTag string
}

func (r egressTestRoute) GetOutboundTag() string { return r.outTag }
func (r egressTestRoute) GetRuleTag() string     { return r.ruleTag }

func egressTestContext(user *protocol.MemoryUser) (context.Context, *session.Outbound) {
	ob := new(session.Outbound)
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{User: user})
	return session.ContextWithOutbounds(ctx, []*session.Outbound{ob}), ob
}

// 固定出口的用户只让 guard: 规则（节点默认安全规则）插队，普通路由规则一律不改变出口。
func TestEgressTagIgnoresNonGuardRoutes(t *testing.T) {
	dedicated := &egressTestHandler{tag: "dedicated"}
	fallback := &egressTestHandler{tag: "fallback"}
	router := &egressTestRouter{route: egressTestRoute{outTag: "fallback", ruleTag: "some-rule"}}
	d := &DefaultDispatcher{
		ohm:    &egressTestManager{egress: dedicated, fallback: fallback},
		router: router,
	}
	ctx, ob := egressTestContext(&protocol.MemoryUser{EgressTag: "dedicated"})
	d.routedDispatch(ctx, &transport.Link{}, net.TCPDestination(net.LocalHostIP, 443))
	if ob.Tag != "dedicated" {
		t.Fatalf("selected outbound = %q, want dedicated", ob.Tag)
	}
}

func TestGuardRuleOverridesEgressTag(t *testing.T) {
	dedicated := &egressTestHandler{tag: "dedicated"}
	block := &egressTestHandler{tag: "block"}
	router := &egressTestRouter{route: egressTestRoute{outTag: "block", ruleTag: "guard:private"}}
	m := &egressTestManager{egress: dedicated, fallback: block}
	d := &DefaultDispatcher{ohm: &guardTestManager{egressTestManager: m, block: block}, router: router}
	ctx, ob := egressTestContext(&protocol.MemoryUser{EgressTag: "dedicated"})
	d.routedDispatch(ctx, &transport.Link{}, net.TCPDestination(net.LocalHostIP, 25))
	if ob.Tag != "block" {
		t.Fatalf("guard 规则没有优先于固定出口：选中 %q", ob.Tag)
	}
}

type guardTestManager struct {
	*egressTestManager
	block outbound.Handler
}

func (m *guardTestManager) GetHandler(tag string) outbound.Handler {
	if tag == m.block.Tag() {
		return m.block
	}
	return m.egressTestManager.GetHandler(tag)
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
