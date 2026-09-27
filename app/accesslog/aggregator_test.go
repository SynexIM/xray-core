package accesslog

import (
	"testing"

	pb "github.com/xtls/xray-core/app/accesslog/command"
)

// drainIngest 直接灌窗口（绕过 channel），等价 run goroutine 消费，确定性断言聚合结果。
func drainIngest(a *Aggregator, evs ...Event) {
	for _, ev := range evs {
		a.ingest(ev)
	}
}

func findSite(ia *pb.InstanceAccess, domain, ip string) *pb.SiteVisit {
	for _, s := range ia.GetSites() {
		if s.GetDomain() == domain && s.GetIp() == ip {
			return s
		}
	}
	return nil
}

func findInstance(report *pb.AccessReport, email string) *pb.InstanceAccess {
	for _, ia := range report.GetInstances() {
		if ia.GetEmail() == email {
			return ia
		}
	}
	return nil
}

// 同一 (email, domain, ip) 多次访问应聚合成一条 SiteVisit：count 累加，first/last 取边界。
func TestAggregateCountFirstLast(t *testing.T) {
	a := New(16, 100)
	drainIngest(a,
		Event{Email: "u1", Domain: "example.com", DestIP: "1.2.3.4", UnixSec: 1000},
		Event{Email: "u1", Domain: "example.com", DestIP: "1.2.3.4", UnixSec: 1005},
		Event{Email: "u1", Domain: "example.com", DestIP: "1.2.3.4", UnixSec: 1003},
		Event{Email: "u1", Domain: "example.com", DestIP: "1.2.3.4", UnixSec: 990},
	)
	report := a.snapshot(true, 0)
	ia := findInstance(report, "u1")
	if ia == nil {
		t.Fatal("missing instance u1")
	}
	if len(ia.GetSites()) != 1 {
		t.Fatalf("want 1 site, got %d", len(ia.GetSites()))
	}
	s := ia.GetSites()[0]
	if s.GetCount() != 4 {
		t.Errorf("count: want 4, got %d", s.GetCount())
	}
	if s.GetFirstSeen() != 990 {
		t.Errorf("firstSeen: want 990, got %d", s.GetFirstSeen())
	}
	if s.GetLastSeen() != 1005 {
		t.Errorf("lastSeen: want 1005, got %d", s.GetLastSeen())
	}
	if s.GetUpBytes() != 0 || s.GetDownBytes() != 0 {
		t.Errorf("bytes must be 0, got up=%d down=%d", s.GetUpBytes(), s.GetDownBytes())
	}
}

// 不同 (domain, ip) 是不同站点；空 domain（纯 IP 直连）单独成站点。
func TestDistinctSites(t *testing.T) {
	a := New(16, 100)
	drainIngest(a,
		Event{Email: "u1", Domain: "a.com", DestIP: "1.1.1.1", UnixSec: 10},
		Event{Email: "u1", Domain: "b.com", DestIP: "2.2.2.2", UnixSec: 11},
		Event{Email: "u1", Domain: "", DestIP: "3.3.3.3", UnixSec: 12},
		Event{Email: "u1", Domain: "a.com", DestIP: "9.9.9.9", UnixSec: 13},
	)
	report := a.snapshot(true, 0)
	ia := findInstance(report, "u1")
	if ia == nil {
		t.Fatal("missing instance u1")
	}
	if len(ia.GetSites()) != 4 {
		t.Fatalf("want 4 distinct sites, got %d", len(ia.GetSites()))
	}
	if findSite(ia, "", "3.3.3.3") == nil {
		t.Error("missing IP-only site")
	}
	if findSite(ia, "a.com", "9.9.9.9") == nil {
		t.Error("missing a.com@9.9.9.9")
	}
}

// 每 email 站点上限，溢出计入 dropped，不静默丢失。
func TestSiteCapOverflowCountsDropped(t *testing.T) {
	const cap = 3
	a := New(16, cap)
	drainIngest(a,
		Event{Email: "u1", Domain: "s1.com", DestIP: "1.0.0.1", UnixSec: 1},
		Event{Email: "u1", Domain: "s2.com", DestIP: "1.0.0.2", UnixSec: 2},
		Event{Email: "u1", Domain: "s3.com", DestIP: "1.0.0.3", UnixSec: 3},
		Event{Email: "u1", Domain: "s4.com", DestIP: "1.0.0.4", UnixSec: 4},
		Event{Email: "u1", Domain: "s5.com", DestIP: "1.0.0.5", UnixSec: 5},
	)
	drainIngest(a, Event{Email: "u1", Domain: "s1.com", DestIP: "1.0.0.1", UnixSec: 6})
	report := a.snapshot(true, 0)
	if report.GetDropped() != 2 {
		t.Errorf("dropped: want 2, got %d", report.GetDropped())
	}
	ia := findInstance(report, "u1")
	if len(ia.GetSites()) != cap {
		t.Fatalf("want %d capped sites, got %d", cap, len(ia.GetSites()))
	}
	if s := findSite(ia, "s1.com", "1.0.0.1"); s == nil || s.GetCount() != 2 {
		t.Errorf("s1 count: want 2, got %v", s)
	}
}

// reset snapshot 后窗口与 dropped 清零。
func TestSnapshotResetsWindow(t *testing.T) {
	a := New(16, 1)
	drainIngest(a,
		Event{Email: "u1", Domain: "a.com", DestIP: "1.1.1.1", UnixSec: 1},
		Event{Email: "u1", Domain: "b.com", DestIP: "2.2.2.2", UnixSec: 2},
	)
	first := a.snapshot(true, 0)
	if first.GetDropped() != 1 {
		t.Fatalf("first dropped: want 1, got %d", first.GetDropped())
	}
	if got := a.snapshot(true, 0); len(got.GetInstances()) != 0 || got.GetDropped() != 0 {
		t.Fatalf("second snapshot: want empty clean window, got %+v", got)
	}
	drainIngest(a, Event{Email: "u2", Domain: "c.com", DestIP: "3.3.3.3", UnixSec: 3})
	third := a.snapshot(true, 0)
	if third.GetDropped() != 0 {
		t.Errorf("third dropped: want 0, got %d", third.GetDropped())
	}
	if findInstance(third, "u1") != nil {
		t.Error("u1 leaked into third window")
	}
	if findInstance(third, "u2") == nil {
		t.Error("missing u2")
	}
}

// 无 email 事件丢弃，不计 dropped。
func TestCollectDropsEmptyEmail(t *testing.T) {
	a := New(16, 100)
	a.Collect(Event{Email: "", Domain: "x.com", DestIP: "1.1.1.1", UnixSec: 1})
	if got := a.dropped.Load(); got != 0 {
		t.Errorf("empty-email must not count as dropped, got %d", got)
	}
	select {
	case <-a.events:
		t.Error("empty-email event must not be enqueued")
	default:
	}
}

// channel 满时 Collect 非阻塞丢弃并 dropped++。
func TestCollectBoundedChannelDrops(t *testing.T) {
	a := New(2, 100)
	a.Collect(Event{Email: "u1", DestIP: "1", UnixSec: 1})
	a.Collect(Event{Email: "u1", DestIP: "2", UnixSec: 2})
	a.Collect(Event{Email: "u1", DestIP: "3", UnixSec: 3})
	a.Collect(Event{Email: "u1", DestIP: "4", UnixSec: 4})
	if got := a.dropped.Load(); got != 2 {
		t.Errorf("bounded-channel drops: want 2, got %d", got)
	}
}

// topN 折叠：超出 topN 的长尾归 (other)，count 累加。
func TestSnapshotTopNFold(t *testing.T) {
	a := New(64, 1000)
	drainIngest(a,
		Event{Email: "u1", Domain: "hot1", DestIP: "1", UnixSec: 1},
		Event{Email: "u1", Domain: "hot1", DestIP: "1", UnixSec: 2},
		Event{Email: "u1", Domain: "hot1", DestIP: "1", UnixSec: 3}, // count 3
		Event{Email: "u1", Domain: "hot2", DestIP: "2", UnixSec: 1},
		Event{Email: "u1", Domain: "hot2", DestIP: "2", UnixSec: 2}, // count 2
		Event{Email: "u1", Domain: "tail1", DestIP: "3", UnixSec: 5},
		Event{Email: "u1", Domain: "tail2", DestIP: "4", UnixSec: 6},
	)
	report := a.snapshot(true, 2)
	ia := findInstance(report, "u1")
	if ia == nil {
		t.Fatal("missing u1")
	}
	if len(ia.GetSites()) != 3 { // top2 + (other)
		t.Fatalf("want 3 sites (top2 + other), got %d", len(ia.GetSites()))
	}
	other := findSite(ia, "(other)", "")
	if other == nil {
		t.Fatal("missing (other) fold")
	}
	if other.GetCount() != 2 { // tail1(1) + tail2(1)
		t.Errorf("(other) count: want 2, got %d", other.GetCount())
	}
}
