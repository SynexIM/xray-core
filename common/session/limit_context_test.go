package session_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/session"
)

func TestSpliceMarkerReachesParentAndCopiedInbound(t *testing.T) {
	parent := session.ContextWithInbound(context.Background(), &session.Inbound{})
	copyInbound := *session.InboundFromContext(parent)
	child := session.ContextWithInbound(context.WithValue(parent, struct{}{}, "child"), &copyInbound)
	if session.SpliceDisabled(parent) {
		t.Fatal("未挂桶时标记不应启用")
	}
	session.DisableSplice(child)
	if !session.SpliceDisabled(parent) || !session.SpliceDisabled(child) {
		t.Fatal("Vision 外层看不到子流标记")
	}
	other := session.ContextWithInbound(context.Background(), &session.Inbound{})
	if session.SpliceDisabled(other) {
		t.Fatal("标记影响了另一条连接")
	}
}
