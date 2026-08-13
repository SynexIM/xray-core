package proxy

import (
	"testing"

	"github.com/xtls/xray-core/common/protocol"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requiresBufferedCopy(tt.user); got != tt.want {
				t.Fatalf("requiresBufferedCopy() = %v, want %v", got, tt.want)
			}
		})
	}
}
