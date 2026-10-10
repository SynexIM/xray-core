package router

import (
	"context"
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	routing_session "github.com/xtls/xray-core/features/routing/session"
)

func userRule(email, out string) *RoutingRule {
	return &RoutingRule{
		TargetTag: &RoutingRule_Tag{Tag: out},
		RuleTag:   "rule-" + email,
		UserEmail: []string{email},
	}
}

func userCtx(email string) *routing_session.Context {
	return &routing_session.Context{
		Inbound:  &session.Inbound{User: &protocol.MemoryUser{Email: email}},
		Outbound: &session.Outbound{Target: net.TCPDestination(net.DomainAddress("example.com"), 443)},
	}
}

func userRules(n int) []*RoutingRule {
	rules := make([]*RoutingRule, 0, n)
	for i := 0; i < n; i++ {
		rules = append(rules, userRule(fmt.Sprintf("user%d@x", i), fmt.Sprintf("out%d", i%7)))
	}
	return rules
}

// BenchmarkPickRouteByUser measures the per-connection cost of resolving a
// per-user outbound when N single-user rules are loaded. The probed user is the
// last one (worst case for a linear scan).
func BenchmarkPickRouteByUser(b *testing.B) {
	for _, n := range []int{100, 1000, 5000, 20000} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			r := new(Router)
			common.Must(r.Init(context.Background(), &Config{Rule: userRules(n)}, nil, nil, nil))
			ctx := userCtx(fmt.Sprintf("user%d@x", n-1))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.PickRoute(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkAddRuleOneByOne is the existing wire behaviour: one AddRule per rule.
func BenchmarkAddRuleOneByOne(b *testing.B) {
	for _, n := range []int{1000, 5000, 20000} {
		b.Run(fmt.Sprintf("rules=%d", n), func(b *testing.B) {
			msgs := make([]*serial.TypedMessage, n)
			for i, rr := range userRules(n) {
				msgs[i] = serial.ToTypedMessage(&Config{Rule: []*RoutingRule{rr}})
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := new(Router)
				common.Must(r.Init(context.Background(), &Config{}, nil, nil, nil))
				for _, m := range msgs {
					if err := r.AddRule(m, true); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
