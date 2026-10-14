package limit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"github.com/xtls/xray-core/transport/pipe"
)

func testRuntime(t *testing.T, profiles []*Profile, rules []*Rule) *Runtime {
	t.Helper()
	r, err := New(&Config{Profiles: profiles, Rules: rules})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}
func attach(t *testing.T, r *Runtime, info featurelimit.SessionInfo) (*pacedReader, *pacedWriter, func()) {
	t.Helper()
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{})
	up, down, release, err := r.Attach(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	if up == nil {
		return nil, nil, release
	}
	return up(&buf.MultiBufferContainer{}).(*pacedReader), down(buf.Discard).(*pacedWriter), release
}
func TestIndexUnionAndConditions(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p"}}, []*Rule{
		{RuleTag: "all", Profile: "p", User: []string{"a", "b"}, InboundTag: []string{"in"}, OutboundTag: []string{"out"}},
		{RuleTag: "user", Profile: "p", User: []string{"a"}},
		{RuleTag: "out", Profile: "p", OutboundTag: []string{"out"}},
	})
	cases := []struct {
		info featurelimit.SessionInfo
		want int
	}{
		{featurelimit.SessionInfo{Email: "a", InboundTag: "in", OutboundTag: "out"}, 3},
		{featurelimit.SessionInfo{Email: "b", InboundTag: "in", OutboundTag: "out"}, 2},
		{featurelimit.SessionInfo{Email: "a", InboundTag: "wrong", OutboundTag: "out"}, 2},
		{featurelimit.SessionInfo{InboundTag: "in", OutboundTag: "out"}, 1},
		{featurelimit.SessionInfo{}, 0},
	}
	for _, c := range cases {
		up, _, release := attach(t, r, c.info)
		count := 0
		if up != nil {
			count = len(up.pacer.flows)
		}
		if count != c.want {
			t.Fatalf("%+v 命中 %d，期望 %d", c.info, count, c.want)
		}
		release()
	}
	if err := r.RemoveRules([]string{"all"}); err != nil {
		t.Fatal(err)
	}
	if len(r.snapshot.Load().inbounds) != 0 || len(r.snapshot.Load().users["b"]) != 0 {
		t.Fatal("增量删规则留下索引")
	}
	if err := r.AddRules([]*Rule{{RuleTag: "user", Profile: "p", User: []string{"c"}}}); err != nil {
		t.Fatal(err)
	}
	if len(r.snapshot.Load().users["a"]) != 0 || len(r.snapshot.Load().users["c"]) != 1 {
		t.Fatal("替换规则没有原子更新索引")
	}
}
func TestSharedRuleAndExistingConnectionSemantics(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(8_000_000)}, {Name: "q", Up: ladder(2_000_000)}}, []*Rule{{RuleTag: "r", Profile: "p", User: []string{"a", "b", "c", "d"}}})
	var original *Shaper
	var releases []func()
	for _, email := range []string{"a", "b", "c", "d"} {
		up, _, release := attach(t, r, featurelimit.SessionInfo{Email: email})
		releases = append(releases, release)
		if original == nil {
			original = up.pacer.flows[0].s
		} else if original != up.pacer.flows[0].s {
			t.Fatal("同规则没有共享桶")
		}
	}
	for _, release := range releases {
		defer release()
	}
	if err := r.PutProfiles([]*Profile{{Name: "p", Up: ladder(4_000_000)}}); err != nil {
		t.Fatal(err)
	}
	if original.Snapshot().RateBps != 4_000_000 {
		t.Fatal("profile 热改没到旧连接")
	}
	if err := r.AddRules([]*Rule{{RuleTag: "r", Profile: "q", User: []string{"a"}}}); err != nil {
		t.Fatal(err)
	}
	fresh, _, release := attach(t, r, featurelimit.SessionInfo{Email: "a"})
	defer release()
	if original == fresh.pacer.flows[0].s || original.Snapshot().RateBps != 4_000_000 || fresh.pacer.flows[0].s.Snapshot().RateBps != 2_000_000 {
		t.Fatal("规则替换改变了旧连接的桶")
	}
	var inUse *ProfileInUseError
	if err := r.RemoveProfiles([]string{"q"}); !errors.As(err, &inUse) || len(inUse.Rules) != 1 || inUse.Rules[0] != "r" {
		t.Fatalf("没有列出引用规则: %v", err)
	}
}
func TestConnectionAndSourceIPAdmissionIsAtomic(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "wide", MaxConnections: 10, MaxSourceIps: 2}, {Name: "small", MaxConnections: 1}}, []*Rule{
		{RuleTag: "a", Profile: "wide", InboundTag: []string{"in"}},
		{RuleTag: "b", Profile: "small", User: []string{"a"}},
	})
	_, _, release := attach(t, r, featurelimit.SessionInfo{Email: "a", InboundTag: "in", SourceIP: "127.0.0.2"})
	_, _, _, err := r.Attach(context.Background(), featurelimit.SessionInfo{Email: "a", InboundTag: "in", SourceIP: "127.0.0.3"})
	var rejected *AdmissionError
	if !errors.As(err, &rejected) || rejected.Resource != "connections" {
		t.Fatalf("没有拒绝超连接数: %v", err)
	}
	state, _ := r.GetRuleStatus("a")
	if state.Connections != 1 || state.SourceIPs != 1 {
		t.Fatal("多桶拒绝后留下槽位或来源 IP")
	}
	release()
	_, _, nextRelease := attach(t, r, featurelimit.SessionInfo{Email: "b", InboundTag: "in", SourceIP: "127.0.0.3"})
	defer nextRelease()
	if _, _, _, err = r.Attach(context.Background(), featurelimit.SessionInfo{Email: "c", InboundTag: "in", SourceIP: "127.0.0.4"}); err == nil {
		t.Fatal("第三个来源没有被拒绝")
	}
	b := r.rules["a"].bucket
	b.mu.Lock()
	for _, ip := range b.ips {
		ip.last = time.Now().Add(-21 * time.Second)
	}
	b.mu.Unlock()
	_, _, thirdRelease := attach(t, r, featurelimit.SessionInfo{Email: "c", InboundTag: "in", SourceIP: "127.0.0.4"})
	thirdRelease()
}
func TestPacedPacketBoundaryAndLifecycle(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(8_000_000), Down: ladder(8_000_000)}}, []*Rule{{RuleTag: "r", Profile: "p", InboundTag: []string{"in"}}})
	up, down, release, err := r.Attach(context.Background(), featurelimit.SessionInfo{InboundTag: "in", Datagram: true})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	reader, writer := pipe.New()
	paced := up(reader).(*pacedReader)
	packet := buf.New()
	packet.Write(make([]byte, 4000))
	dest := net.UDPDestination(net.LocalHostIP, 53)
	packet.UDP = &dest
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{packet}); err != nil {
		t.Fatal(err)
	}
	got, err := paced.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Len() != 4000 || got[0].UDP != &dest {
		t.Fatal("UDP/XUDP 报文被拆分或丢了目的地址")
	}
	buf.ReleaseMulti(got)
	paced.ReturnAnError(errors.New("xudp"))
	if err := paced.Recover(); err == nil {
		t.Fatal("Recover 没传到底层")
	}
	commonReader, commonWriter := pipe.New()
	wrapped := down(commonWriter).(*pacedWriter)
	wrapped.Interrupt()
	if _, err := commonReader.ReadMultiBuffer(); err == nil {
		t.Fatal("Writer 的 Interrupt 没传到底层")
	}
	paced.Interrupt()
	if _, err := reader.ReadMultiBuffer(); err == nil {
		t.Fatal("Reader 的 Interrupt 没传到底层")
	}
}
func TestConcurrentRuleAndProfileUpdates(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(8_000_000)}}, []*Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}})
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, _, release, err := r.Attach(context.Background(), featurelimit.SessionInfo{Email: "a", SourceIP: "127.0.0.1"})
				if err != nil {
					t.Error(err)
					return
				}
				release()
				r.GetRuleStatus("r")
				r.ListRules(0, 10)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			r.PutProfiles([]*Profile{{Name: "p", Up: ladder(4_000_000)}})
			r.AddRules([]*Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}})
			r.tick(time.Now())
		}
	}()
	wg.Wait()
}

func TestNodeMeterIncludesUnmatchedTrafficWithoutDoubleCounting(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Down: ladder(8_000_000)}}, []*Rule{
		{RuleTag: "a", Profile: "p", User: []string{"a"}},
		{RuleTag: "b", Profile: "p", OutboundTag: []string{"out"}},
	})
	if err := r.SetNode(&NodeLimit{CapacityBps: 12000, EnterPercent: 85, ExitPercent: 70}); err != nil {
		t.Fatal(err)
	}
	_, matched, release := attach(t, r, featurelimit.SessionInfo{Email: "a", OutboundTag: "out"})
	defer release()
	if err := matched.WriteMultiBuffer(buf.MergeBytes(nil, make([]byte, 1000))); err != nil {
		t.Fatal(err)
	}
	_, unmatched, release2 := attach(t, r, featurelimit.SessionInfo{Email: "other"})
	defer release2()
	if len(unmatched.pacer.flows) != 0 {
		t.Fatal("未命中连接创建了速率桶")
	}
	if err := unmatched.WriteMultiBuffer(buf.MergeBytes(nil, make([]byte, 500))); err != nil {
		t.Fatal(err)
	}
	r.tick(r.lastTick.Add(time.Second))
	status := r.NodeStatus()
	if status.DownloadBps != 12000 || status.UtilizationPercent != 100 || !status.Congested {
		t.Fatalf("节点计量漏算或重复: %+v", status)
	}
}

func TestRetiredBucketKeepsCreditUntilFull(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(8_000_000, 800_000)}}, []*Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}})
	b := r.rules["r"].bucket
	_, _, release := attach(t, r, featurelimit.SessionInfo{Email: "a"})
	b.up.mu.Lock()
	b.up.grantLocked(b.up.NewFlow(), 100_000)
	b.up.mu.Unlock()
	release()
	if err := r.RemoveRules([]string{"r"}); err != nil {
		t.Fatal(err)
	}
	r.tick(time.Now().Add(time.Second))
	if _, ok := r.buckets[b]; !ok {
		t.Fatal("额度没回满就丢了退役桶")
	}
	b.up.mu.Lock()
	b.up.advance(time.Now().Add(time.Minute))
	b.up.mu.Unlock()
	b.down.mu.Lock()
	b.down.advance(time.Now().Add(time.Minute))
	b.down.mu.Unlock()
	r.tick(time.Now().Add(2 * time.Minute))
	if _, ok := r.buckets[b]; ok {
		t.Fatal("回满后退役桶没回收")
	}
}
