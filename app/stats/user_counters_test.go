package stats_test

import (
	"context"
	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/protocol"
	featurestats "github.com/xtls/xray-core/features/stats"
	"sync"
	"testing"
)

func TestIdentityStatsReferencesAndReclamation(t *testing.T) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	first := &protocol.MemoryUser{Email: "same"}
	second := &protocol.MemoryUser{Email: "same"}
	for _, user := range []*protocol.MemoryUser{first, second} {
		if err := user.PrepareStats(manager, true, true, true); err != nil {
			t.Fatal(err)
		}
	}
	if first.Stats() != second.Stats() {
		t.Fatal("同名身份未共用统计")
	}
	first.Stats().Uplink().Add(42)
	siteName := "user>>>same>>>site>>>example.test>>>traffic>>>uplink"
	site, err := first.Stats().SiteCounter(siteName)
	if err != nil {
		t.Fatal(err)
	}
	site.Add(7)
	first.Stats().Online().AddIP("192.0.2.1")
	first.RevokeRuntimeSessions()
	if manager.GetCounter(featurestats.UserTrafficCounterName("same", "uplink")) == nil {
		t.Fatal("仍被另一个入站使用的计数器被删除")
	}
	second.RevokeRuntimeSessions()
	if manager.GetCounter(siteName) != nil {
		t.Fatal("站点计数器没有回收")
	}
	if _, err := second.Stats().SiteCounter(siteName); err == nil {
		t.Fatal("已撤销的句柄重建了站点计数器")
	}
	if manager.GetCounter(featurestats.UserTrafficCounterName("same", "uplink")) != nil || manager.GetOnlineMap(featurestats.UserOnlineMapName("same")) != nil {
		t.Fatal("删除后仍持有统计资源")
	}
	changed := 0
	manager.VisitChangedUsers(nil, func(string, featurestats.UserCounters) { changed++ })
	if changed != 0 {
		t.Fatal("变化集合持有已删除身份")
	}
}

func TestPreparedIdentityStatsDoNotAllocate(t *testing.T) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	user := &protocol.MemoryUser{Email: "prepared"}
	if err := user.PrepareStats(manager, true, true, true); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		if err := user.PrepareStats(manager, true, true, true); err != nil {
			t.Fatal(err)
		}
		user.Stats().Uplink().Add(1)
	})
	if allocations != 0 {
		t.Fatalf("已有句柄仍分配内存: %f", allocations)
	}
}
func TestChangedSetConcurrentExportDoesNotLoseTraffic(t *testing.T) {
	manager, err := appstats.NewManager(context.Background(), &appstats.Config{})
	if err != nil {
		t.Fatal(err)
	}
	user := &protocol.MemoryUser{Email: "active"}
	if err := user.PrepareStats(manager, true, true, false); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	var total int64
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < 10000; index++ {
				user.Stats().Uplink().Add(1)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wait.Wait(); close(done) }()
	for {
		manager.VisitChangedUsers(nil, func(_ string, counters featurestats.UserCounters) { total += counters.Uplink().Set(0) })
		select {
		case <-done:
			manager.VisitChangedUsers(nil, func(_ string, counters featurestats.UserCounters) { total += counters.Uplink().Set(0) })
			if total != 80000 {
				t.Fatalf("流量丢失: %d", total)
			}
			return
		default:
		}
	}
}
