package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

func TestEnforceConnLimitReservesUntilContextCancel(t *testing.T) {
	user := &protocol.MemoryUser{Email: "alice@example.test", ConnLimit: 1}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: user})

	if err := enforceConnLimit(ctx); err != nil {
		t.Fatalf("first connection should reserve slot: %v", err)
	}
	if err := enforceConnLimit(ctx); err == nil {
		t.Fatal("second connection should exceed conn limit")
	}

	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		nextCtx, nextCancel := context.WithCancel(context.Background())
		nextCtx = session.ContextWithInbound(nextCtx, &session.Inbound{User: user})
		err := enforceConnLimit(nextCtx)
		nextCancel()
		if err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection slot was not released after context cancellation")
}
