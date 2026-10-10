package socks

import (
	"bytes"
	"context"
	"io"
	gonet "net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/proxy/http"
	"github.com/xtls/xray-core/transport/internet"
)

func TestBindRejectedWithoutListener(t *testing.T) {
	var listens atomic.Int32
	if err := internet.RegisterListenerController(func(string, string, syscall.RawConn) error {
		listens.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// First prove that the counter sees real system listeners.
	probe, err := internet.ListenSystemPacket(context.Background(), &gonet.UDPAddr{IP: gonet.IPv4(127, 0, 0, 1)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	probe.Close()
	if listens.Load() == 0 {
		t.Fatal("listener counter did not observe probe")
	}
	for _, auth := range []AuthType{AuthType_NO_AUTH, AuthType_PASSWORD} {
		for _, udp := range []bool{false, true} {
			config := &ServerConfig{AuthType: auth, UdpEnabled: udp, Accounts: map[string]string{"u": "p"}}
			session := &ServerSession{config: config, users: http.NewUserStore(config.UserLevel, config.Accounts, config.UserAccounts), localAddress: net.LocalHostIP}
			input := []byte{5, 1, 0}
			responseLength := 12
			if auth == AuthType_PASSWORD {
				input = []byte{5, 1, 2, 1, 1, 'u', 1, 'p'}
				responseLength = 14
			}
			// RFC 1928 BIND with the 0.0.0.0:0 placeholder.
			input = append(input, 5, 2, 0, 1, 0, 0, 0, 0, 0, 0)
			server, client := gonet.Pipe()
			server.SetDeadline(time.Now().Add(3 * time.Second))
			client.SetDeadline(time.Now().Add(3 * time.Second))
			before := listens.Load()
			done := make(chan error, 1)
			go func() {
				defer server.Close()
				req, udpConn, err := session.Handshake(bytes.NewReader(input), server)
				if req != nil || udpConn != nil {
					t.Error("BIND created a request or UDP listener")
				}
				done <- err
			}()
			response := make([]byte, responseLength)
			_, readErr := io.ReadFull(client, response)
			client.Close()
			if err := <-done; err == nil {
				t.Fatal("BIND was accepted")
			}
			if readErr != nil {
				t.Fatal(readErr)
			}
			if !bytes.Equal(response[len(response)-10:], []byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}) {
				t.Fatalf("response: %x", response)
			}
			if listens.Load() != before {
				t.Fatal("BIND opened a listener")
			}
		}
	}
}
