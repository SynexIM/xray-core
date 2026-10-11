package router

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/testing/mocks"
)

// Replacing a balancer rebinds the rules that point at it; removing one that
// is still used fails, removing an unused one succeeds.
func TestPutBalancerRebindsRules(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	hs := mocks.NewOutboundHandlerSelector(mockCtl)
	hs.EXPECT().Select([]string{"a-"}).Return([]string{"a-1"}).AnyTimes()
	hs.EXPECT().Select([]string{"b-"}).Return([]string{"b-1"}).AnyTimes()

	r := new(Router)
	common.Must(r.Init(context.TODO(), &Config{
		Rule:          []*RoutingRule{{TargetTag: &RoutingRule_BalancingTag{BalancingTag: "lb"}, RuleTag: "r1", Networks: []net.Network{net.Network_TCP}}},
		BalancingRule: []*BalancingRule{{Tag: "lb", OutboundSelector: []string{"a-"}}},
	}, mocks.NewDNSClient(mockCtl), &selectingManager{Manager: mocks.NewOutboundManager(mockCtl), HandlerSelector: hs}, nil))

	pick := func() string {
		ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)}})
		route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
		common.Must(err)
		return route.GetOutboundTag()
	}
	if got := pick(); got != "a-1" {
		t.Fatalf("before: %s", got)
	}
	common.Must(r.PutBalancers([]*BalancingRule{{Tag: "lb", OutboundSelector: []string{"b-"}}}))
	if got := pick(); got != "b-1" {
		t.Fatalf("after replace: %s", got)
	}
	if err := r.RemoveBalancers([]string{"lb"}); err == nil {
		t.Fatal("removing a balancer still used by r1 must fail")
	}
	common.Must(r.PutBalancers([]*BalancingRule{{Tag: "spare", OutboundSelector: []string{"a-"}}}))
	common.Must(r.RemoveBalancers([]string{"spare", "missing"}))
	if _, ok := (*r.balancers.Load())["spare"]; ok {
		t.Fatal("spare not removed")
	}
}

type selectingManager struct {
	outbound.Manager
	outbound.HandlerSelector
}
