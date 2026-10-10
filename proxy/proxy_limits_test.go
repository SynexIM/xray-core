package proxy

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
)

func TestRequiresBufferedCopyForGovernedUser(t *testing.T) {
	tests := []struct {
		name string
		user *protocol.MemoryUser
		want bool
	}{
		{name: "anonymous user", want: false},
		{name: "unlimited user", user: &protocol.MemoryUser{}, want: false},
		{name: "bandwidth limited user", user: &protocol.MemoryUser{BandwidthBps: 10_000_000}, want: true},
		{name: "connection limited user", user: &protocol.MemoryUser{ConnLimit: 200}, want: true},
		// 只配了承诺速率的用户 BandwidthBps 是 0。漏判 = 走 splice = 限速一个字节都不生效。
		{name: "committed-rate only user", user: &protocol.MemoryUser{CommittedBps: 8_000_000}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresBufferedCopy(tt.user); got != tt.want {
				t.Fatalf("requiresBufferedCopy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManagedOutboundDisablesSplice(t *testing.T) {
	tests := []struct {
		name      string
		outbounds []*session.Outbound
		want      bool
	}{
		{name: "no outbound metadata", want: false},
		{name: "ordinary outbound keeps splice", outbounds: []*session.Outbound{{CanSpliceCopy: 1}}, want: false},
		{name: "protocol rejects splice", outbounds: []*session.Outbound{{CanSpliceCopy: 3}}, want: true},
		{name: "managed rate limit rejects splice", outbounds: []*session.Outbound{{CanSpliceCopy: 1, ForceBufferedCopy: true}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := outboundRequiresBufferedCopy(tt.outbounds); got != tt.want {
				t.Fatalf("outboundRequiresBufferedCopy() = %v, want %v", got, tt.want)
			}
		})
	}
}
