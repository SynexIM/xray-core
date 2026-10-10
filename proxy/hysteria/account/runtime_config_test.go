package account

import (
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"testing"
)

func TestVirtualRouteAliasKeepsLimitsAndHotUpdates(t *testing.T) {
	p := &protocol.User{Email: "hy-alias@example.test", Pool: "hy-alias-pool", ConnLimit: 1, UploadBandwidthBps: 8_000_000, DownloadBandwidthBps: 16_000_000, EgressTag: "eg-original", Account: serial.ToTypedMessage(&Account{Auth: "00000000-0000-4000-8000-000000000001"})}
	u, err := p.ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}
	validator := NewValidator()
	if err = validator.Add(u); err != nil {
		t.Fatal(err)
	}
	alias := validator.Get("00000000-0000-7000-8000-000000000001")
	if alias == nil || alias.RuntimeConfig().Pool != p.Pool || !alias.HasRuntimeLimits() {
		t.Fatal("virtual route dropped runtime configuration")
	}
	parsed, err := protocol.ToProtoUser(alias).GetTypedAccount()
	if err != nil || parsed.(*MemoryAccount).Auth != "00000000-0000-7000-8000-000000000001" {
		t.Fatal("runtime serialization lost alias account")
	}
	release, ok := u.AcquireRuntimeConnection()
	if !ok {
		t.Fatal("initial slot rejected")
	}
	defer release()
	if done, allowed := alias.AcquireRuntimeConnection(); allowed {
		done()
		t.Fatal("virtual route multiplied connection quota")
	}
	p.DownloadBandwidthBps = 24_000_000
	p.EgressTag = "eg-updated"
	n, err := p.ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}
	u.AdoptLimits(n)
	if alias.RuntimeConfig().DownloadBandwidthBps != 24_000_000 || alias.RuntimeConfig().EgressTag != "eg-updated" {
		t.Fatal("virtual route kept stale limits or egress")
	}
	u.ResetRuntimeConnections()
}
