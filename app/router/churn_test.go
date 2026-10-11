package router

import (
	"testing"

	"github.com/xtls/xray-core/common/serial"
)

func TestBatchRouteOrderAndAtomicFailure(t *testing.T) {
	r := &Router{}
	r.rules.Store(newRuleSet([]*Rule{{RuleTag: "first"}, {RuleTag: "middle"}, {RuleTag: "last"}}))
	r.balancers.Store(&map[string]*Balancer{})
	config := &Config{Rule: []*RoutingRule{
		{RuleTag: "middle", UserEmail: []string{"alice"}, TargetTag: &RoutingRule_Tag{Tag: "new"}},
		{RuleTag: "added", UserEmail: []string{"bob"}, TargetTag: &RoutingRule_Tag{Tag: "new"}},
	}}
	if err := r.ReplaceRule(serial.ToTypedMessage(config)); err != nil {
		t.Fatal(err)
	}
	got := r.rules.Load().all
	for i, tag := range []string{"first", "middle", "last", "added"} {
		if got[i].RuleTag != tag {
			t.Fatalf("position %d: %s", i, got[i].RuleTag)
		}
	}
	before := r.rules.Load()
	config.Rule[1].RuleTag = "middle"
	if err := r.ReplaceRule(serial.ToTypedMessage(config)); err == nil {
		t.Fatal("accepted duplicate replacement")
	}
	if r.rules.Load() != before {
		t.Fatal("failed batch changed snapshot")
	}
	errs := r.RemoveRules([]string{"first", "", "last", "missing"})
	if errs[0] != nil || errs[1] == nil || errs[2] != nil || errs[3] != nil {
		t.Fatalf("results: %v", errs)
	}
	got = r.rules.Load().all
	if len(got) != 2 || got[0].RuleTag != "middle" || got[1].RuleTag != "added" {
		t.Fatal("delete changed survivor order")
	}
}
