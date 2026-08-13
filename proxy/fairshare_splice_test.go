package proxy

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
)

// 公平调度启用时必须强制 buffered copy：splice 零拷贝会绕过 dispatcher 挂的
// FairLimit 包装器（无限速用户逃公平整形且不进活跃字节统计）。
func TestRequiresBufferedCopyWhenFairShareEnabled(t *testing.T) {
	sched := protocol.FairScheduler()
	old := sched.AvailBps()
	defer sched.SetNodeBandwidth(old)

	sched.SetNodeBandwidth(0) // 公平关闭
	if requiresBufferedCopy(&protocol.MemoryUser{}) {
		t.Error("fair disabled + no per-user limits: splice must stay available")
	}
	if !requiresBufferedCopy(&protocol.MemoryUser{BandwidthBps: 1000}) {
		t.Error("per-user bandwidth limit must force buffered copy")
	}
	if !requiresBufferedCopy(&protocol.MemoryUser{ConnLimit: 1}) {
		t.Error("per-user conn limit must force buffered copy")
	}

	sched.SetNodeBandwidth(1_000_000) // 公平开启
	if !requiresBufferedCopy(&protocol.MemoryUser{}) {
		t.Error("fair enabled: unlimited user must also take buffered path (no splice bypass)")
	}
	if !requiresBufferedCopy(nil) {
		t.Error("fair enabled: nil user must also take buffered path")
	}
}
