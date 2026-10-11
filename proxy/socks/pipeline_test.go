package socks

import (
	"errors"
	"io"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
)

func pipelineRequest() *protocol.RequestHeader {
	return &protocol.RequestHeader{
		Version: socks5Version, Command: protocol.RequestCommandTCP,
		Address: net.DomainAddress("example.com"), Port: 80,
		User: &protocol.MemoryUser{Account: &Account{Username: "u", Password: "p"}},
	}
}

// strictServer reads each stage only after answering the previous one, like a
// real server; tolerant decides whether bytes beyond the current stage survive.
func strictServer(t *testing.T, conn gonet.Conn, tolerant bool) {
	defer conn.Close()
	buf := make([]byte, 512)
	n, err := conn.Read(buf)
	if err != nil {
		return
	}
	rest := buf[3:n] // beyond the 3-byte greeting
	conn.Write([]byte{socks5Version, authPassword})
	if !tolerant {
		rest = nil // drops whatever came with the greeting, then waits for auth
	}
	need := 1 + 1 + 1 + 1 + 1 // ver ulen u plen p
	for len(rest) < need {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		rest = append(rest, buf[:n]...)
	}
	conn.Write([]byte{0x01, 0x00})
	conn.Write([]byte{socks5Version, 0x00, 0x00, 0x01, 1, 2, 3, 4, 0, 80})
	io.Copy(io.Discard, conn)
}

func TestPipelinedHandshake(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tolerant bool
	}{{"accepts pipelining", true}, {"drops pipelined bytes", false}} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := gonet.Pipe()
			defer client.Close()
			go strictServer(t, server, tc.tolerant)
			start := time.Now()
			_, err := ClientHandshakePipelined(pipelineRequest(), client, client, time.Now().Add(10*time.Second))
			var rejected *pipelineRejected
			switch {
			case tc.tolerant && err != nil:
				t.Fatalf("handshake: %v", err)
			case !tc.tolerant && !errors.As(err, &rejected):
				t.Fatalf("want pipelineRejected, got %v", err)
			case !tc.tolerant && time.Since(start) > 3*time.Second:
				t.Fatalf("stalled %v before giving up; want ~1s", time.Since(start))
			}
		})
	}
}
