package router

import (
	"context"
	"google.golang.org/protobuf/proto"
	"github.com/xtls/xray-core/common/serial"
	"testing"
)

func TestNativeRuleSnapshotPreservesConditionsAndIsIsolated(t *testing.T) {
	r := new(Router)
	rule := &RoutingRule{RuleTag: "guard:one", TargetTag: &RoutingRule_Tag{Tag: "block"}, Protocol: []string{"tls"}, UserEmail: []string{"demo"}, Attributes: map[string]string{"host": "example"}}
	if err := r.Init(context.Background(), &Config{Rule: []*RoutingRule{rule}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	rule.Protocol[0] = "http"
	var snapshot RoutingRule
	if err := proto.Unmarshal(r.RuleConfigByTag()["guard:one"], &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Protocol[0] != "tls" || snapshot.Attributes["host"] != "example" {
		t.Fatal("runtime native conditions were lost or caller mutation leaked")
	}
	snapshot.Protocol[0] = "udp"
	var second RoutingRule
	_ = proto.Unmarshal(r.RuleConfigByTag()["guard:one"], &second)
	if second.Protocol[0] != "tls" {
		t.Fatal("snapshot mutation changed live routing state")
	}
}

func TestAtomicReplaceRejectsBadRuleAndKeepsGuard(t *testing.T) {
	r:=new(Router); original:=&RoutingRule{RuleTag:"guard:one",TargetTag:&RoutingRule_Tag{Tag:"block"},Protocol:[]string{"tls"}}
	if err:=r.Init(context.Background(),&Config{Rule:[]*RoutingRule{original}},nil,nil,nil); err!=nil { t.Fatal(err) }
	bad:=&RoutingRule{RuleTag:"guard:one",TargetTag:&RoutingRule_Tag{Tag:"direct"}}
	if err:=r.ReplaceRule(serial.ToTypedMessage(&Config{Rule:[]*RoutingRule{bad}})); err==nil { t.Fatal("empty condition accepted") }
	var actual RoutingRule; _=proto.Unmarshal(r.RuleConfigByTag()["guard:one"],&actual)
	if !proto.Equal(original,&actual) { t.Fatal("bad update removed existing guard") }
	next:=proto.Clone(original).(*RoutingRule); next.Protocol=[]string{"http"}
	if err:=r.ReplaceRule(serial.ToTypedMessage(&Config{Rule:[]*RoutingRule{next}})); err!=nil { t.Fatal(err) }
	_=proto.Unmarshal(r.RuleConfigByTag()["guard:one"],&actual); if !proto.Equal(next,&actual) || len(r.ListRule())!=1 { t.Fatal("replacement duplicated or failed to update guard") }
}
