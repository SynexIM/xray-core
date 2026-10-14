package udp

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/dataplane"
	"github.com/xtls/xray-core/common/net"
	protocoludp "github.com/xtls/xray-core/common/protocol/udp"
	"github.com/xtls/xray-core/transport/internet"
)

func TestDispatcherOverflowCountsDrops(t *testing.T) {
	packetConnection, err := DialDispatcher(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	connection := packetConnection.(*dispatcherConn)
	defer connection.Close()
	counter := dataplane.Counter("dataplane>>>udp_drop>>>dispatcher")
	before := counter.Value()
	for packet := 0; packet <= dataplane.UDPDispatcherPackets; packet++ {
		connection.callback(context.Background(), &protocoludp.Packet{Payload: buf.FromBytes([]byte("packet")), Source: net.UDPDestination(net.LocalHostIP, 1234)})
	}
	if counter.Value() != before+1 {
		t.Fatal("dispatcher 满队列没有计数")
	}
	for len(connection.cache) > 0 {
		if _, _, err := connection.ReadFrom(make([]byte, 10)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHubOverflowCountsDrops(t *testing.T) {
	hub, err := ListenUDP(context.Background(), net.LocalHostIP, 0, &internet.MemoryStreamConfig{}, HubCapacity(1))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		hub.Close()
		for packet := range hub.Receive() {
			packet.Payload.Release()
		}
	}()
	connection, err := net.DialUDP("udp", nil, hub.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	counter := dataplane.Counter("dataplane>>>udp_drop>>>hub")
	before := counter.Value()
	for packet := 0; packet < 64; packet++ {
		if _, err := connection.Write([]byte("packet")); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for counter.Value() == before && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if counter.Value() == before {
		t.Fatal("hub 满队列没有计数")
	}
}
