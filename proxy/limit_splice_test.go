package proxy

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/limit"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	featurelimit "github.com/xtls/xray-core/features/limit"
)

func TestLimitMarksMatchedSessionsToDisableSplice(t *testing.T) {
	r, err := limit.New(&limit.Config{Profiles: []*limit.Profile{{Name: "p", Down: &limit.Ladder{Steps: []*limit.Step{{RateBps: 5_000_000}}}}}, Rules: []*limit.Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, email := range []string{"a", "b", ""} {
		inbound := &session.Inbound{User: &protocol.MemoryUser{Email: email}, CanSpliceCopy: 1}
		ctx := session.ContextWithInbound(context.Background(), inbound)
		_, _, release, err := r.Attach(ctx, featurelimit.SessionInfo{Email: email})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		if !session.SpliceDisabled(ctx) {
			t.Fatalf("计量或整形连接允许绕过 link: %s", email)
		}
	}
}
