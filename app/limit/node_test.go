package limit

import (
	"context"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"testing"
	"time"
)

func TestCongestionHysteresisAndDisabledNode(t *testing.T) {
	state := &congestionState{}
	node := &NodeLimit{CapacityBps: 100_000_000, EnterPercent: 85, ExitPercent: 70, HoldSeconds: 5}
	start := time.Now()
	if updateCongestion(state, node, 3_750_000, start) {
		t.Fatal("30Mbps 不应拥挤")
	}
	if !updateCongestion(state, node, 11_250_000, start) {
		t.Fatal("90Mbps 没有进入拥挤")
	}
	if !updateCongestion(state, node, 5_000_000, start.Add(time.Second)) {
		t.Fatal("提前退出")
	}
	if updateCongestion(state, node, 5_000_000, start.Add(6*time.Second)) {
		t.Fatal("持续低于退出阈值没有退出")
	}
	if updateCongestion(state, nil, 100_000_000, start) {
		t.Fatal("容量未配置却调度")
	}
}
func TestWeightedWaterFillAndHeavyPriority(t *testing.T) {
	slots := []*fairSlot{
		{key: "normal", weight: 1, ceiling: 80, want: 80, floor: 10},
		{key: "heavy", weight: 1, ceiling: 80, want: 80, floor: 10, heavy: true},
	}
	fill(slots, 100)
	byKey := map[string]float64{}
	for _, slot := range slots {
		byKey[slot.key] = slot.alloc
	}
	if byKey["normal"] != 80 || byKey["heavy"] != 20 {
		t.Fatalf("正常桶未优先: %+v %+v", slots[0], slots[1])
	}
	slots = []*fairSlot{{key: "a", weight: 1, ceiling: 100, want: 100}, {key: "b", weight: 3, ceiling: 100, want: 100}}
	fill(slots, 100)
	if slots[0].alloc != 25 || slots[1].alloc != 75 {
		t.Fatalf("权重没有生效: %v %v", slots[0].alloc, slots[1].alloc)
	}
}
func TestShortHeavyWindowDoesNotBecomeFifteenSeconds(t *testing.T) {
	var w usageWindow
	w.add(100, 1)
	if w.average() != 100 {
		t.Fatalf("1 秒窗口被扩大: %v", w.average())
	}
	w.add(0, 1)
	if w.average() != 0 {
		t.Fatal("窗口没自然过期")
	}
}

func TestNonDivisibleHeavyWindowKeepsConfiguredDuration(t *testing.T) {
	var w usageWindow
	for range 40 {
		w.add(100, 17)
	}
	if w.average() != 100 {
		t.Fatalf("17 秒窗口被扩大: %v", w.average())
	}
	for range 19 {
		w.add(0, 17)
	}
	if w.average() != 0 {
		t.Fatal("旧流量没有按窗口过期")
	}
}
func TestProfileWithoutCongestedDoesNotParticipate(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(80_000_000)}}, []*Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}})
	b := r.rules["r"].bucket
	b.up.mu.Lock()
	b.up.granted = 10_000_000
	b.up.waits = 1
	b.up.mu.Unlock()
	r.direction([]*bucket{b}, 1_000_000, 1_000_000, 1, true, true)
	if b.up.Snapshot().RateBps != 80_000_000 {
		t.Fatal("没有拥挤参数的桶被注水")
	}
}

func TestCongestedAllocationReservesUnmanagedTraffic(t *testing.T) {
	r := testRuntime(t, []*Profile{{Name: "p", Up: ladder(80_000_000), Congested: &Congested{Weight: 1}}}, []*Rule{{RuleTag: "r", Profile: "p", User: []string{"a"}}})
	_, _, release, err := r.Attach(context.Background(), featurelimit.SessionInfo{Email: "a"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if err := r.SetNode(&NodeLimit{CapacityBps: 100_000_000, EnterPercent: 85, ExitPercent: 70}); err != nil {
		t.Fatal(err)
	}
	b := r.rules["r"].bucket
	b.up.mu.Lock()
	b.up.granted = 2_500_000
	b.up.waits = 1
	b.up.mu.Unlock()
	r.upBytes.Store(12_500_000)
	r.participatingUp.Store(2_500_000)
	r.tick(r.lastTick.Add(time.Second))
	if rate := b.up.Snapshot().RateBps; rate != 20_000_000 {
		t.Fatalf("80Mbps 非参与流量没从容量扣除: %d", rate)
	}
}
