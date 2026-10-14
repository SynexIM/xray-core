package hysteria

import (
	"testing"

	"github.com/xtls/xray-core/common/dataplane"
)

func TestSessionOverflowCountsDrops(t *testing.T) {
	manager := &udpSessionManager{m: map[uint32]*InterConn{7: {ch: make(chan []byte, 1)}}}
	counter := dataplane.Counter("dataplane>>>udp_drop>>>hysteria")
	before := counter.Value()
	manager.feed(7, []byte("accepted"))
	manager.feed(7, []byte("dropped"))
	if counter.Value() != before+1 {
		t.Fatal("Hysteria 满队列没有计数")
	}
	if string(<-manager.m[7].ch) != "accepted" {
		t.Fatal("满队列改变了已接受数据")
	}
}
