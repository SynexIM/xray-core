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

func TestActiveConnectionGaugesReleaseOnContextCancel(t *testing.T) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	user := &protocol.MemoryUser{Email: "client-uid"}
	ctx, cancel := context.WithCancel(session.ContextWithInbound(context.Background(), &session.Inbound{
		Tag: "shared-inbound", User: user,
	}))
	if err := trackConnectionStats(ctx, manager); err != nil {
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
	user := &protocol.MemoryUser{Email: "client-uid"}
	failed := session.ContextWithInbound(context.Background(), &session.Inbound{User: user})
	if err := trackConnectionStats(failed, failingCounterManager{}); err == nil {
		t.Fatal("expected gauge registration failure")
	}
	ctx, cancel := context.WithCancel(session.ContextWithInbound(context.Background(), &session.Inbound{User: user}))
	defer cancel()
	if err := trackConnectionStats(ctx, featurestats.NoopManager{}); err != nil {
		t.Fatalf("failed gauge registration leaked the connection slot: %v", err)
	}
}
