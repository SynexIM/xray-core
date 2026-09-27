package dispatcher

import (
	"context"
	"errors"
	"testing"
	"time"

	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	featurestats "github.com/xtls/xray-core/features/stats"
)

func TestEnforceConnLimitReservesUntilContextCancel(t *testing.T) {
	user := &protocol.MemoryUser{Email: "alice@example.test", ConnLimit: 1}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: user})

	if err := enforceConnLimit(ctx, featurestats.NoopManager{}); err != nil {
		t.Fatalf("first connection should reserve slot: %v", err)
	}
	if err := enforceConnLimit(ctx, featurestats.NoopManager{}); err == nil {
		t.Fatal("second connection should exceed conn limit")
	}

	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		nextCtx, nextCancel := context.WithCancel(context.Background())
		nextCtx = session.ContextWithInbound(nextCtx, &session.Inbound{User: user})
		err := enforceConnLimit(nextCtx, featurestats.NoopManager{})
		nextCancel()
		if err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection slot was not released after context cancellation")
}

func TestActiveConnectionGaugesReleaseOnContextCancel(t *testing.T) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	user := &protocol.MemoryUser{Email: "client-uid", ConnLimit: 1}
	ctx, cancel := context.WithCancel(session.ContextWithInbound(context.Background(), &session.Inbound{
		Tag: "shared-inbound", User: user,
	}))
	if err := enforceConnLimit(ctx, manager); err != nil {
		t.Fatal(err)
	}
	userCounter := manager.GetCounter(featurestats.ActiveUserConnectionCounterName("client-uid"))
	inboundCounter := manager.GetCounter(featurestats.ActiveConnectionCounterName("shared-inbound"))
	if userCounter == nil || inboundCounter == nil || userCounter.Value() != 1 || inboundCounter.Value() != 1 {
		t.Fatalf("unexpected active gauges: user=%v inbound=%v", userCounter, inboundCounter)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (userCounter.Value() != 0 || inboundCounter.Value() != 0) {
		time.Sleep(time.Millisecond)
	}
	if userCounter.Value() != 0 || inboundCounter.Value() != 0 {
		t.Fatalf("active gauges leaked after cancellation: user=%d inbound=%d", userCounter.Value(), inboundCounter.Value())
	}
}

type failingCounterManager struct{ featurestats.NoopManager }

func (failingCounterManager) GetOrRegisterCounter(string) (featurestats.Counter, error) {
	return nil, errors.New("unavailable")
}

func TestGaugeRegistrationFailureReleasesConnectionSlot(t *testing.T) {
	user := &protocol.MemoryUser{Email: "client-uid", ConnLimit: 1}
	failed := session.ContextWithInbound(context.Background(), &session.Inbound{User: user})
	if err := enforceConnLimit(failed, failingCounterManager{}); err == nil {
		t.Fatal("expected gauge registration failure")
	}
	ctx, cancel := context.WithCancel(session.ContextWithInbound(context.Background(), &session.Inbound{User: user}))
	defer cancel()
	if err := enforceConnLimit(ctx, featurestats.NoopManager{}); err != nil {
		t.Fatalf("failed gauge registration leaked the connection slot: %v", err)
	}
}
