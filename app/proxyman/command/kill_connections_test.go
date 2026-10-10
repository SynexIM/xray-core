package command

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
)

func TestKillUserConnectionsPreservesIdentityAndOtherUsers(t *testing.T) {
	identity := &protocol.MemoryUser{Email: "kill-target"}
	other := &protocol.MemoryUser{Email: "kill-other"}
	var stopped, otherStopped atomic.Uint32
	release, ok := identity.TrackRuntimeSession(func() { stopped.Add(1) })
	if !ok {
		t.Fatal("登记失败")
	}
	defer release()
	otherRelease, ok := other.TrackRuntimeSession(func() { otherStopped.Add(1) })
	if !ok {
		t.Fatal("登记失败")
	}
	defer otherRelease()
	server := &handlerServer{}
	if _, err := server.KillUserConnections(context.Background(), &KillUserConnectionsRequest{Emails: []string{identity.Email, identity.Email}}); err != nil {
		t.Fatal(err)
	}
	if stopped.Load() != 1 || otherStopped.Load() != 0 {
		t.Fatal("断连未按 email 隔离或重复取消")
	}
	nextRelease, ok := identity.TrackRuntimeSession(func() { stopped.Add(1) })
	if !ok {
		t.Fatal("断连接口撤销了身份")
	}
	defer nextRelease()
	identity.RevokeRuntimeSessions()
	if stopped.Load() != 2 {
		t.Fatal("后续删除没有断新会话")
	}
	if _, ok := identity.TrackRuntimeSession(func() {}); ok {
		t.Fatal("撤销后旧身份仍能建立会话")
	}
}
