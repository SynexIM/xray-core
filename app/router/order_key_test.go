package router

import (
	"fmt"
	"testing"

	"github.com/xtls/xray-core/common/serial"
)

func TestStableOrderKeyAndIncrementalReplacement(t *testing.T) {
	r := &Router{}
	r.rules.Store(newRuleSet(nil))
	r.balancers.Store(&map[string]*Balancer{})
	makeRule := func(tag, key, target string) *RoutingRule {
		return &RoutingRule{RuleTag: tag, OrderKey: key, UserEmail: []string{"alice"}, TargetTag: &RoutingRule_Tag{Tag: target}}
	}
	for _, rule := range []*RoutingRule{makeRule("late", "b", "late-out"), makeRule("retry", "a", "early-out")} {
		if errs := r.AddRules([]*serial.TypedMessage{serial.ToTypedMessage(&Config{Rule: []*RoutingRule{rule}})}, true); errs[0] != nil {
			t.Fatal(errs[0])
		}
	}
	if got := pick(t, r, "alice"); got != "early-out" {
		t.Fatalf("较早创建的规则重试后失去顺序：%s", got)
	}
	if n := r.rules.Load().others.Len(); n != 0 {
		t.Fatalf("排序元数据使纯用户规则退出索引：%d", n)
	}
	before := r.rules.Load()
	replacement := makeRule("retry", "a", "changed-out")
	if err := r.ReplaceRule(serial.ToTypedMessage(&Config{Rule: []*RoutingRule{replacement}})); err != nil {
		t.Fatal(err)
	}
	if got := pick(t, r, "alice"); got != "changed-out" {
		t.Fatal(got)
	}
	// 旧快照不能被新写入修改；读者可以继续无锁使用它。
	if before.byTag.Len() != 2 || before.allRules()[0].Tag != "early-out" {
		t.Fatal("写入修改了已发布的快照")
	}
	replacement.Priority = -10
	if err := r.ReplaceRule(serial.ToTypedMessage(&Config{Rule: []*RoutingRule{replacement}})); err != nil {
		t.Fatal(err)
	}
	if got := pick(t, r, "alice"); got != "late-out" {
		t.Fatalf("改优先级未重新排序：%s", got)
	}
}

// 不是小表基准：先真实装入十万条用户规则，再量单条原子替换的完整路径。
func BenchmarkReplaceRule100K(b *testing.B) {
	r := &Router{}
	r.rules.Store(newRuleSet(nil))
	r.balancers.Store(&map[string]*Balancer{})
	rules := make([]*RoutingRule, 100000)
	for i := range rules {
		name := fmt.Sprintf("u-%06d", i)
		rules[i] = &RoutingRule{RuleTag: name, OrderKey: name, UserEmail: []string{name}, TargetTag: &RoutingRule_Tag{Tag: "direct"}}
	}
	if err := r.ReloadRules(&Config{Rule: rules}, false); err != nil {
		b.Fatal(err)
	}
	message := serial.ToTypedMessage(&Config{Rule: []*RoutingRule{rules[len(rules)/2]}})
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := r.ReplaceRule(message); err != nil {
			b.Fatal(err)
		}
	}
}
