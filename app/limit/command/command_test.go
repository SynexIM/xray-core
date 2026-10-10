package command

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/limit"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func constantProfile(name string, rate uint64) *limit.Profile {
	return &limit.Profile{Name: name, Up: &limit.Ladder{Steps: []*limit.Step{{RateBps: rate}}}, Down: &limit.Ladder{Steps: []*limit.Step{{RateBps: rate}}}}
}
func TestLimitServiceOverGRPC(t *testing.T) {
	runtime, err := limit.New(&limit.Config{Profiles: []*limit.Profile{constantProfile("p", 2_000_000)}, Rules: []*limit.Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	RegisterLimitServiceServer(server, &limitServer{controller: runtime})
	go server.Serve(listener)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "passthrough:///limit", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewLimitServiceClient(conn)
	_, _, release, err := runtime.Attach(ctx, featurelimit.SessionInfo{Email: "a", SourceIP: "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err = client.PutProfiles(ctx, &PutProfilesRequest{Profiles: []*limit.Profile{constantProfile("p", 5_000_000), constantProfile("q", 3_000_000)}}); err != nil {
		t.Fatal(err)
	}
	state, err := client.GetStatus(ctx, &GetStatusRequest{RuleTag: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if state.Rule.Down.CurrentRateBps != 5_000_000 || state.Rule.Down.CurrentStep != 1 || state.Rule.Connections != 1 || state.Rule.SourceIps != 1 || len(state.Rule.Down.RemainingBytes) != 1 {
		t.Fatalf("存量桶或状态失真: %v", state)
	}
	_, err = client.RemoveProfiles(ctx, &RemoveProfilesRequest{Names: []string{"p", "q"}})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "r") {
		t.Fatalf("未报告引用者: %v", err)
	}
	_, err = client.PutProfiles(ctx, &PutProfilesRequest{Profiles: []*limit.Profile{constantProfile("p", 8_000_000), constantProfile("bad", 0)}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("无效 profile 没有正确错误码: %v", err)
	}
	state, err = client.GetStatus(ctx, &GetStatusRequest{RuleTag: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if state.Rule.Down.CurrentRateBps != 5_000_000 {
		t.Fatal("无效批次部分更新了 profile")
	}
	_, err = client.AddRules(ctx, &AddRulesRequest{Rules: []*limit.Rule{{RuleTag: "new", Profile: "p", User: []string{"b"}}, {RuleTag: "bad", Profile: "missing", User: []string{"c"}}}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("未知引用没被拒绝: %v", err)
	}
	listed, err := client.ListRules(ctx, &ListRulesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if listed.Total != 1 {
		t.Fatal("无效规则批次部分生效")
	}
	if _, err = client.AddRules(ctx, &AddRulesRequest{Rules: []*limit.Rule{{RuleTag: "r", Profile: "q", User: []string{"a"}}, {RuleTag: "s", Profile: "p", InboundTag: []string{"in"}}}}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListRules(ctx, &ListRulesRequest{Offset: 1, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if listed.Total != 2 || len(listed.Rules) != 1 || listed.Rules[0].RuleTag != "s" || listed.Rules[0].Profile != "p" {
		t.Fatalf("分页失真: %v", listed)
	}
	state, err = client.GetStatus(ctx, &GetStatusRequest{RuleTag: "r"})
	if err != nil {
		t.Fatal(err)
	}
	if state.Rule.Profile != "q" || state.Rule.Connections != 0 {
		t.Fatal("替换后没有创建新规则桶")
	}
	_, err = client.GetStatus(ctx, &GetStatusRequest{RuleTag: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("查不到规则的错误码不正确: %v", err)
	}
	_, err = client.SetNode(ctx, &SetNodeRequest{Node: &limit.NodeLimit{CapacityBps: 10_000_000, EnterPercent: 101}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("错误节点阈值没被拒绝")
	}
	if _, err = client.SetNode(ctx, &SetNodeRequest{Node: &limit.NodeLimit{CapacityBps: 10_000_000, EnterPercent: 85, ExitPercent: 70, HoldSeconds: 5}}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RemoveRules(ctx, &RemoveRulesRequest{RuleTags: []string{"r", "s"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = client.RemoveProfiles(ctx, &RemoveProfilesRequest{Names: []string{"p", "q"}}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListRules(ctx, &ListRulesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if listed.Total != 0 || len(listed.Rules) != 0 {
		t.Fatal("批量删除没有收口")
	}
}
