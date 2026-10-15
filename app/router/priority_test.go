package router

import (
	"context"
	"slices"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	routing_session "github.com/xtls/xray-core/features/routing/session"
)

// 优先级大的先匹配；同优先级按加入先后。用户索引里的规则同样服从优先级。
func TestRulePriority(t *testing.T) {
	r := &Router{}
	r.rules.Store(newRuleSet(nil))
	r.balancers.Store(&map[string]*Balancer{})
	add := func(rules ...*RoutingRule) {
		t.Helper()
		if errs := r.AddRules([]*serial.TypedMessage{serial.ToTypedMessage(&Config{Rule: rules})}, true); errs[0] != nil {
			t.Fatal(errs[0])
		}
	}
	add(&RoutingRule{RuleTag: "catch-all", Priority: -100, InboundTag: []string{"in"}, TargetTag: &RoutingRule_Tag{Tag: "default"}})
	add(&RoutingRule{RuleTag: "alice", UserEmail: []string{"alice"}, TargetTag: &RoutingRule_Tag{Tag: "alice-out"}})
	add(&RoutingRule{RuleTag: "deny", Priority: 1000, InboundTag: []string{"in"}, UserEmail: []string{"alice"}, TargetTag: &RoutingRule_Tag{Tag: "reject"}})
	add(&RoutingRule{RuleTag: "bob", UserEmail: []string{"bob"}, TargetTag: &RoutingRule_Tag{Tag: "bob-out"}})

	var order []string
	for _, rule := range r.rules.Load().allRules() {
		order = append(order, rule.RuleTag)
	}
	if want := []string{"deny", "alice", "bob", "catch-all"}; !slices.Equal(order, want) {
		t.Fatalf("顺序 %v，期望 %v", order, want)
	}
	for user, want := range map[string]string{"alice": "reject", "bob": "bob-out", "carol": "default"} {
		got := pick(t, r, user)
		if got != want {
			t.Fatalf("%s 走了 %s，期望 %s", user, got, want)
		}
	}
}

func pick(t *testing.T, r *Router, user string) string {
	t.Helper()
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{Tag: "in", User: &protocol.MemoryUser{Email: user}})
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	rule := r.rules.Load().pick(routing_session.AsRoutingContext(ctx))
	if rule == nil {
		return ""
	}
	tag, _ := rule.GetTag()
	return tag
}
