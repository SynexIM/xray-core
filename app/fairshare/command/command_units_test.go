package command

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
)

// 下发面的速率全是字节/秒，原样进调度器。改成除 8 的症状是「节点被掐到 1/8 速度」。
func TestSetNodeBandwidthKeepsBytesPerSecond(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetNodeBandwidth(0); sched.SetCongestionHysteresis(0, 0, 0) })

	if _, err := s.SetNodeBandwidth(context.Background(), &SetNodeBandwidthRequest{AvailBps: 60_000_000}); err != nil {
		t.Fatal(err)
	}
	if got := sched.RootCapBytePerSec(); got != 60_000_000 {
		t.Fatalf("root_cap 必须原样是字节/秒: want 60000000, got %d", got)
	}
}

// class 表整份替换，字段一一对应；0 原样是 0。
func TestSetClassPolicyMapsFieldsAndReplacesWholeTable(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetClassPolicies(nil) })

	if _, err := s.SetClassPolicy(context.Background(), &SetClassPolicyRequest{Classes: []*ClassPolicy{
		{Name: "c1", Weight: 3, FloorBytePerSec: 125_000, DownloadReservedBytePerSec: 1_000_000, HeavyWindowSeconds: 900, HeavyPercent: 80},
		{Name: "c2"},
	}}); err != nil {
		t.Fatal(err)
	}
	c1, c2 := sched.ClassPolicyFor("c1"), sched.ClassPolicyFor("c2")
	if c1 == nil || c1.Weight != 3 || c1.FloorBytePerSec != 125_000 || c1.DownloadReservedBytePerSec != 1_000_000 ||
		c1.UploadReservedBytePerSec != 0 || c1.HeavyWindowSeconds != 900 || c1.HeavyPercent != 80 {
		t.Fatalf("c1 映射错了: %+v", c1)
	}
	if c2 == nil || *c2 != (protocol.ClassPolicy{Name: "c2"}) {
		t.Fatalf("没给的字段必须是 0，不能被换成默认值: %+v", c2)
	}
	if _, err := s.SetClassPolicy(context.Background(), &SetClassPolicyRequest{Classes: []*ClassPolicy{{Name: "c2", Weight: 2}}}); err != nil {
		t.Fatal(err)
	}
	if p := sched.ClassPolicyFor("c1"); p != nil {
		t.Errorf("整份替换后 c1 应被删除，got %+v", p)
	}
}

// 字段错位比没有这个接口更糟——上层会照着错的数字切链路容量。
func TestGetStatusMapsSchedulerState(t *testing.T) {
	s := NewFairShareServer()
	sched := protocol.FairScheduler()
	t.Cleanup(func() { sched.SetNodeBandwidth(0) })

	if _, err := s.SetNodeBandwidth(context.Background(), &SetNodeBandwidthRequest{AvailBps: 60_000_000}); err != nil {
		t.Fatal(err)
	}
	resp, err := s.GetStatus(context.Background(), &GetStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	st := sched.Status()
	if resp.GetRootCapBytePerSec() != 60_000_000 ||
		resp.GetCongested() != st.Congested ||
		resp.GetActiveMembers() != st.ActiveMembers ||
		resp.GetHeavyMembers() != st.HeavyMembers ||
		resp.GetUsedUploadBytePerSec() != st.UsedUploadBytePerSec ||
		resp.GetUsedDownloadBytePerSec() != st.UsedDownloadBytePerSec ||
		resp.GetFillTruncated() != st.FillTruncated ||
		resp.GetFillRounds() != st.FillRounds {
		t.Errorf("字段错位：resp=%+v scheduler=%+v", resp, st)
	}
}
