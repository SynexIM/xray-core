package router_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/testing/mocks"
)

// ListRule builds Routes with no routing.Context — a rule is not a connection.
// GetUser/GetInboundTag are promoted from that nil interface, so before they were
// overridden this listing panicked and took the whole process down. ListRuleFull
// over gRPC calls exactly these two, which made it unusable for drift detection.
func TestListRuleAnswersFromRuleConditionsWithoutContext(t *testing.T) {
	config := &Config{
		Rule: []*RoutingRule{
			{
				TargetTag:  &RoutingRule_Tag{Tag: "out-a"},
				RuleTag:    "ipl_cust_1",
				UserEmail:  []string{"cust1@ipl"},
				InboundTag: []string{"in-vless"},
			},
			{
				// No user/inbound condition at all — it must answer empty, not panic.
				TargetTag: &RoutingRule_Tag{Tag: "out-b"},
				RuleTag:   "ipl_cust_2",
				Networks:  []net.Network{net.Network_TCP},
			},
		},
	}

	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	r := new(Router)
	common.Must(r.Init(context.TODO(), config, mocks.NewDNSClient(mockCtl), &mockOutboundManager{
		Manager:         mocks.NewOutboundManager(mockCtl),
		HandlerSelector: mocks.NewOutboundHandlerSelector(mockCtl),
	}, nil))

	var rules []routing.Route
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("ListRule panicked: %v", recovered)
			}
		}()
		rules = r.ListRule()
		for _, rule := range rules {
			_ = rule.GetUser()
			_ = rule.GetInboundTag()
		}
	}()

	if len(rules) != 2 {
		t.Fatalf("want 2 rules, got %d", len(rules))
	}
	if got := rules[0].GetUser(); got != "cust1@ipl" {
		t.Errorf("rule 0 user: want cust1@ipl, got %q", got)
	}
	if got := rules[0].GetInboundTag(); got != "in-vless" {
		t.Errorf("rule 0 inbound: want in-vless, got %q", got)
	}
	if got := rules[0].GetOutboundTag(); got != "out-a" {
		t.Errorf("rule 0 outbound: want out-a, got %q", got)
	}
	// A rule carrying no user/inbound condition answers empty, not a panic.
	if got := rules[1].GetUser(); got != "" {
		t.Errorf("rule 1 user: want empty, got %q", got)
	}
	if got := rules[1].GetInboundTag(); got != "" {
		t.Errorf("rule 1 inbound: want empty, got %q", got)
	}
}
