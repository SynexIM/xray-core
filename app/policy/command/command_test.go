package command

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/policy"
)

func TestReplaceLevels(t *testing.T) {
	ctx := context.Background()
	m, err := policy.New(ctx, &policy.Config{Level: map[uint32]*policy.Policy{
		1: {Timeout: &policy.Policy_Timeout{ConnectionIdle: &policy.Second{Value: 300}}},
		2: {Timeout: &policy.Policy_Timeout{ConnectionIdle: &policy.Second{Value: 10}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	s := &policyServer{manager: m}
	old := m.ForLevel(1)
	input := &policy.Policy{Timeout: &policy.Policy_Timeout{ConnectionIdle: &policy.Second{Value: 60}}, Stats: &policy.Policy_Stats{UserUplink: true}}
	if _, err := s.SetLevels(ctx, &SetLevelsRequest{Levels: map[uint32]*policy.Policy{1: input}}); err != nil {
		t.Fatal(err)
	}
	input.Timeout.ConnectionIdle.Value = 5
	input.Stats.UserUplink = false
	if got := m.ForLevel(1); got.Timeouts.ConnectionIdle != 60*time.Second || !got.Stats.UserUplink {
		t.Fatalf("new policy: %+v", got)
	}
	if old.Timeouts.ConnectionIdle != 300*time.Second {
		t.Fatalf("old snapshot changed: %+v", old)
	}
	if m.ForLevel(2).Timeouts.ConnectionIdle != 300*time.Second {
		t.Fatal("omitted level was not removed")
	}
	got, err := s.GetLevels(ctx, &GetLevelsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got.Levels[1].Timeout.ConnectionIdle.Value = 9
	if m.ForLevel(1).Timeouts.ConnectionIdle != 60*time.Second {
		t.Fatal("GetLevels leaked mutable state")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.SetLevels(map[uint32]*policy.Policy{1: {}})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.ForLevel(1)
			m.GetLevels()
		}
	}()
	wg.Wait()
	if _, err := s.SetLevels(ctx, &SetLevelsRequest{}); err != nil {
		t.Fatal(err)
	}
	if len(m.GetLevels()) != 0 {
		t.Fatal("empty replacement did not clear levels")
	}
}
