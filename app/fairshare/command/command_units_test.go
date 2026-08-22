package command

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
)

// 下发面的单位契约（FR-079d）：本服务收到的 `_bps` 全是**字节/秒**，
// 原样进调度器，一次都不除 8。谁要是「顺手统一」成比特/秒，这条会红，
// 而线上的症状会是「节点被掐到 1/8 速度」——那种 bug 从现象倒查回来要几天。
func TestSetNodeBandwidthKeepsBytesPerSecond(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetNodeBandwidth(0); sched.SetFloors(0, 0); sched.SetCongestionHysteresis(0, 0, 0) })

	if _, err := s.SetNodeBandwidth(context.Background(), &SetNodeBandwidthRequest{
		AvailBps:               60_000_000, // 480 Mbps 的线，node-agent 已折算成字节/秒
		SoftFloorBps:           62_500,
		HardFloorBps:           16_384,
		CongestionEnterPercent: 90,
		CongestionExitPercent:  70,
		CongestionExitTicks:    3,
	}); err != nil {
		t.Fatal(err)
	}
	if got := sched.RootCapBytePerSec(); got != 60_000_000 {
		t.Fatalf("root_cap 必须原样是字节/秒: want 60000000, got %d", got)
	}
}

// FR-079c：0 就是「不启用」，不是「悄悄换成一个默认值」。
// 旧实现把 0 换成 62500 / 16384 两个魔数，运营看不出来自己其实开了地板。
func TestZeroFloorsAreNotReplacedByDefaults(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetNodeBandwidth(0); sched.SetFloors(0, 0) })

	if _, err := s.SetNodeBandwidth(context.Background(), &SetNodeBandwidthRequest{AvailBps: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	if soft, hard := sched.FloorsBytePerSec(); soft != 0 || hard != 0 {
		t.Fatalf("不配地板就该是没有地板，got soft=%d hard=%d（旧实现在这里塞了 62500 / 16384 两个魔数）", soft, hard)
	}
}

// class 表整份替换，字段一一对应且带单位后缀，不会译错。
func TestSetClassPolicyMapsFieldsAndReplacesWholeTable(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetClassPolicies(nil) })

	if _, err := s.SetClassPolicy(context.Background(), &SetClassPolicyRequest{Classes: []*ClassPolicy{
		{Name: "live", Weight: 4, NormalCapBytePerSec: 2_500_000, BurstCapBytePerSec: 15_000_000, BurstCreditBytes: 1 << 30, FloorRatioPercent: 50},
		{Name: "short", Weight: 1, NormalCapBytePerSec: 2_500_000},
	}}); err != nil {
		t.Fatal(err)
	}
	live := sched.ClassPolicyFor("live")
	if live == nil || live.Weight != 4 || live.NormalCapBytePerSec != 2_500_000 ||
		live.BurstCapBytePerSec != 15_000_000 || live.BurstCreditBytes != 1<<30 || live.FloorRatioPercent != 50 {
		t.Fatalf("live 策略映射错了: %+v", live)
	}

	// 声明式：没出现在新请求里的 class 就该消失。
	if _, err := s.SetClassPolicy(context.Background(), &SetClassPolicyRequest{Classes: []*ClassPolicy{
		{Name: "short", Weight: 2},
	}}); err != nil {
		t.Fatal(err)
	}
	if p := sched.ClassPolicyFor("live"); p != nil {
		t.Errorf("整份替换后 live 应被删除，got %+v", p)
	}
	if p := sched.ClassPolicyFor("short"); p == nil || p.Weight != 2 {
		t.Errorf("short 应被更新为 weight 2，got %+v", p)
	}
}
