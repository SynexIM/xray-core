//go:build linux

package limit

import (
	"context"
	"github.com/xtls/xray-core/common/session"
	"golang.org/x/sys/unix"
	"math"
	"net"
	"testing"
)

func TestCustomerSocketPacingAndUpdateThreshold(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	pacer, err := newSocketPacer(session.ContextWithInbound(context.Background(), &session.Inbound{Conn: server}))
	if err != nil || pacer == nil {
		t.Fatalf("未取得客户 socket: %v", err)
	}
	readRate := func() uint32 {
		var value int
		var optionError error
		err := pacer.connection.Control(func(descriptor uintptr) {
			value, optionError = unix.GetsockoptInt(int(descriptor), unix.SOL_SOCKET, unix.SO_MAX_PACING_RATE)
		})
		if err != nil {
			t.Fatal(err)
		}
		if optionError != nil {
			t.Fatal(optionError)
		}
		return uint32(value)
	}
	if err := pacer.update(1_000_000); err != nil {
		t.Fatal(err)
	}
	if readRate() != 1_000_000 {
		t.Fatal("socket 速率单位错误")
	}
	if err := pacer.update(1_090_000); err != nil {
		t.Fatal(err)
	}
	if readRate() != 1_000_000 {
		t.Fatal("小于 10% 的变化重设了 socket")
	}
	if err := pacer.update(500_000); err != nil {
		t.Fatal(err)
	}
	if readRate() != 500_000 {
		t.Fatal("份额变化未生效")
	}
	if err := pacer.update(math.Inf(1)); err != nil {
		t.Fatal(err)
	}
	if readRate() != math.MaxUint32 {
		t.Fatal("热改为无限速仍保留旧 socket 上限")
	}
}
