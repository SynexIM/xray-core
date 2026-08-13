package inbound

import (
	"context"
	"testing"

	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
)

func TestCleanupUserStatsRemovesCountersAndOnlineMap(t *testing.T) {
	sm, err := appstats.NewManager(context.Background(), &appstats.Config{})
	common.Must(err)

	const email = "alice@example.test"
	prefix := "user>>>" + email + ">>>"

	uplink, err := sm.RegisterCounter(prefix + "traffic>>>uplink")
	common.Must(err)
	downlink, err := sm.RegisterCounter(prefix + "traffic>>>downlink")
	common.Must(err)
	online, err := sm.RegisterOnlineMap(prefix + "online")
	common.Must(err)

	uplink.Add(123)
	downlink.Add(456)
	online.AddIP("127.0.0.1")

	cleanupUserStats(sm, email)

	if got := sm.GetCounter(prefix + "traffic>>>uplink"); got != nil {
		t.Fatal("uplink counter still registered after cleanup")
	}
	if got := sm.GetCounter(prefix + "traffic>>>downlink"); got != nil {
		t.Fatal("downlink counter still registered after cleanup")
	}
	if got := sm.GetOnlineMap(prefix + "online"); got != nil {
		t.Fatal("online map still registered after cleanup")
	}
}
