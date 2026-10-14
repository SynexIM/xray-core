package wireguard

import (
	"testing"

	"github.com/xtls/xray-core/common/dataplane"
	"github.com/xtls/xray-core/common/net"
)

func TestManagerOverflowCountsDrops(t *testing.T) {
	source := net.UDPDestination(net.LocalHostIP, 12345)
	target := net.UDPDestination(net.LocalHostIP, 54321)
	connection := &udpConn{queue: newPacketQueue(1), src: source, dst: target}
	manager := &udpManager{m: map[string]*udpConn{source.NetAddr(): connection}}
	counter := dataplane.Counter("dataplane>>>udp_drop>>>wireguard")
	before := counter.Value()
	manager.feed(source, target, []byte("accepted"))
	manager.feed(source, target, []byte("dropped"))
	if counter.Value() != before+1 {
		t.Fatal("WireGuard 满队列没有计数")
	}
}
