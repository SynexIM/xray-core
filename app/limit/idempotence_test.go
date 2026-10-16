package limit

import (
	"context"
	"errors"
	"testing"

	featurelimit "github.com/xtls/xray-core/features/limit"
	"google.golang.org/protobuf/proto"
)

func TestRuleRetryKeepsAdmissionAndEmptyInventory(t *testing.T) {
	empty := testRuntime(t, nil, nil)
	if rules, total := empty.ListRules(0, 100); len(rules) != 0 || total != 0 {
		t.Fatal("新实例必须能查询空清单")
	}
	rule := &Rule{RuleTag: "shared", Profile: "p", User: []string{"alice"}}
	r := testRuntime(t, []*Profile{{Name: "p", MaxConnections: 1}}, []*Rule{rule})
	_, _, release := attach(t, r, featurelimit.SessionInfo{Email: "alice", SourceIP: "127.0.0.1"})
	defer release()
	before := r.snapshot.Load()
	bucket := r.rules[rule.RuleTag].bucket
	if err := r.AddRules([]*Rule{proto.Clone(rule).(*Rule)}); err != nil {
		t.Fatal(err)
	}
	if r.snapshot.Load() != before || r.rules[rule.RuleTag].bucket != bucket {
		t.Fatal("原样重试不应发布新快照或更换桶")
	}
	_, _, _, err := r.Attach(context.Background(), featurelimit.SessionInfo{Email: "alice"})
	var rejected *AdmissionError
	if !errors.As(err, &rejected) || rejected.Resource != "connections" {
		t.Fatalf("原样重试后连接限额被洗掉：%v", err)
	}
}
