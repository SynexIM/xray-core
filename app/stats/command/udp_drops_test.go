package command_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/stats"
	. "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/dataplane"
)

func TestUDPDropCountersReachStatsRPC(t *testing.T) {
	manager, err := stats.NewManager(context.Background(), &stats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	server := NewStatsServer(manager)
	names := []string{"pipe", "hub", "dispatcher", "wireguard", "hysteria"}
	for layer, name := range names {
		counterName := "dataplane>>>udp_drop>>>" + name
		before := manager.GetCounter(counterName).Value()
		dataplane.DropUDP(dataplane.UDPLayer(layer), 3)
		result, err := server.GetStats(context.Background(), &GetStatsRequest{Name: counterName})
		if err != nil || result.Stat.Value != before+3 {
			t.Fatalf("%s 未导出丢包数: %v %v", name, result, err)
		}
	}
	result, err := server.QueryStats(context.Background(), &QueryStatsRequest{Pattern: "dataplane>>>udp_drop>>>"})
	if err != nil || len(result.Stat) != 5 {
		t.Fatalf("计数器族不完整: %v %v", result, err)
	}
}
