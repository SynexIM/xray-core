package command_test

import (
	"context"
	"github.com/xtls/xray-core/app/stats"
	. "github.com/xtls/xray-core/app/stats/command"
	"testing"
)

func TestScopedStatsIncludesOfflineTrafficAndDoesNotResetOthers(t *testing.T) {
	manager, err := stats.NewManager(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := manager.RegisterCounter("user>>>demo-owned>>>traffic>>>uplink")
	if err != nil {
		t.Fatal(err)
	}
	owned.Set(42)
	external, err := manager.RegisterCounter("user>>>external>>>traffic>>>uplink")
	if err != nil {
		t.Fatal(err)
	}
	external.Set(99)
	server := NewStatsServer(manager)
	result, err := server.GetUsersStats(context.Background(), &GetUsersStatsRequest{IncludeTraffic: true, Reset_: true, Emails: []string{"demo-owned"}})
	if err != nil || len(result.Users) != 1 || result.Users[0].Email != "demo-owned" || result.Users[0].Traffic.Uplink != 42 || len(result.Users[0].Ips) != 0 {
		t.Fatal("offline scoped counter missing", err)
	}
	if external.Value() != 99 || owned.Value() != 0 {
		t.Fatal("scoped read touched an external identity")
	}
}
