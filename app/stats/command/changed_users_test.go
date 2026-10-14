package command_test

import (
	"context"
	"github.com/xtls/xray-core/app/stats"
	. "github.com/xtls/xray-core/app/stats/command"
	"testing"
)

func TestOnlyChangedIncludesOfflineTrafficAndPreservesOtherSelection(t *testing.T) {
	manager, err := stats.NewManager(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, email := range []string{"first", "second"} {
		counter, err := manager.GetOrRegisterCounter("user>>>" + email + ">>>traffic>>>uplink")
		if err != nil {
			t.Fatal(err)
		}
		counter.Add(42)
	}
	server := NewStatsServer(manager)
	request := &GetUsersStatsRequest{OnlyChanged: true, IncludeTraffic: true, Reset_: true, Emails: []string{"first"}}
	response, err := server.GetUsersStats(context.Background(), request)
	if err != nil || len(response.Users) != 1 || response.Users[0].Traffic.Uplink != 42 {
		t.Fatalf("增量缺少离线流量: %v %v", response, err)
	}
	response, err = server.GetUsersStats(context.Background(), request)
	if err != nil || len(response.Users) != 0 {
		t.Fatalf("清零又产生变化: %v %v", response, err)
	}
	response, err = server.GetUsersStats(context.Background(), &GetUsersStatsRequest{OnlyChanged: true, IncludeTraffic: true})
	if err != nil || len(response.Users) != 1 || response.Users[0].Email != "second" {
		t.Fatalf("未选中的变化被消费: %v %v", response, err)
	}
}
