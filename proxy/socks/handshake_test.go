package socks

import (
	goerrors "errors"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// fakeUpstream behaves like the residential gateways measured on 2026-10-11:
// every message is taken with a single read and anything else in that read is
// dropped. dropEarly also drops data arriving before CONNECT is answered.
type fakeUpstream struct {
	dropEarly bool
	stall     bool // never answers the auth stage
}

func (f fakeUpstream) serve(conn gonet.Conn) {
	defer conn.Close()
	b := make([]byte, 1024)
	read := func() bool { _, err := conn.Read(b); return err == nil }
	if !read() { // greeting
		return
	}
	conn.Write([]byte{socks5Version, authPassword})
	if !read() || f.stall { // auth
		if f.stall {
			time.Sleep(5 * time.Second)
		}
		return
	}
	conn.Write([]byte{0x01, 0x00})
	if !read() { // request; extra bytes in the same read are lost
		return
	}
	time.Sleep(30 * time.Millisecond) // "connecting to the target"
	if f.dropEarly {
		conn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
		for read() {
		}
		conn.SetReadDeadline(time.Time{})
	}
	conn.Write([]byte{socks5Version, 0x00, 0x00, 0x01, 1, 2, 3, 4, 0, 80})
	if read() {
		conn.Write([]byte("HTTP/1.1 204 No Content\r\n\r\n"))
	}
}

func listen(t *testing.T, f fakeUpstream) func() (stat.Connection, error) {
	l, err := gonet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return func() (stat.Connection, error) { return gonet.Dial("tcp", l.Addr().String()) }
}

func testUser() *protocol.MemoryUser {
	return &protocol.MemoryUser{Account: &Account{Username: "u", Password: "p"}}
}

// Staggered stages survive a gateway that drops extra bytes per read; a
// gateway that never answers auth is recognised within about a second.
func TestStaggeredHandshake(t *testing.T) {
	request := &protocol.RequestHeader{Version: socks5Version, Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("example.com"), Port: 80, User: testUser()}
	conn, _ := listen(t, fakeUpstream{})()
	if _, err := staggeredHandshake(request, nil, conn, time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("staggered handshake: %v", err)
	}
	conn.Close()

	conn, _ = listen(t, fakeUpstream{stall: true})()
	start := time.Now()
	_, err := staggeredHandshake(request, nil, conn, time.Now().Add(5*time.Second))
	var stalled *stageStalled
	if !goerrors.As(err, &stalled) || time.Since(start) > 3*time.Second {
		t.Fatalf("want stageStalled within ~1s, got %v after %v", err, time.Since(start))
	}
}

// The self-check turns early data on only where data sent right after CONNECT
// survives, and staggering on for both kinds of gateway.
func TestSelfCheck(t *testing.T) {
	for _, tc := range []struct {
		name      string
		upstream  fakeUpstream
		staggered capState
		early     capState
	}{
		{"keeps early data", fakeUpstream{}, capOn, capOn},
		{"drops early data", fakeUpstream{dropEarly: true}, capOn, capOff},
		{"stalls", fakeUpstream{stall: true}, capOff, capUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			staggered, early := selfCheck(listen(t, tc.upstream), testUser(), false, false)
			if staggered != tc.staggered || early != tc.early {
				t.Fatalf("staggered=%d early=%d, want %d %d", staggered, early, tc.staggered, tc.early)
			}
		})
	}
}
