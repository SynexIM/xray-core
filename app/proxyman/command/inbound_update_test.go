package command_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/proxyman"
	command "github.com/xtls/xray-core/app/proxyman/command"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	_ "github.com/xtls/xray-core/main/distro/all"
	"golang.org/x/net/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestInboundPolicyUpdatePreservesRealXrayUserAndConnection(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "xr-hot-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	go func() {
		for {
			c, err := target.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	socket := filepath.Join(dir, "api.sock")
	raw := fmt.Sprintf(`{"log":{"loglevel":"warning"},"api":{"tag":"api","listen":%q,"services":["HandlerService"]},"inbounds":[{"tag":"hot","listen":"127.0.0.1","port":%d,"protocol":"socks","settings":{"auth":"password","accounts":[{"user":"alice","pass":"local-test","email":"hot-user"}]}}],"outbounds":[{"protocol":"freedom","settings":{"finalRules":[{"action":"allow","ip":["127.0.0.1/32"],"port":%d}]}}]}`, socket, port, target.Addr().(*net.TCPAddr).Port)
	v, err := core.StartInstance("json", []byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := command.NewHandlerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", port), &proxy.Auth{User: "alice", Password: "local-test"}, &net.Dialer{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	live, err := dialer.Dial("tcp", target.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	echo := func(message string) {
		t.Helper()
		live.SetDeadline(time.Now().Add(time.Second))
		if _, err := live.Write([]byte(message)); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, len(message))
		if _, err := io.ReadFull(live, data); err != nil || string(data) != message {
			t.Fatalf("live stream: %q %v", data, err)
		}
	}
	echo("before")
	list, err := client.ListInbounds(ctx, &command.ListInboundsRequest{Tags: []string{"hot"}})
	if err != nil || len(list.GetInbounds()) != 1 {
		t.Fatalf("list: %v", err)
	}
	in := proto.Clone(list.Inbounds[0]).(*core.InboundHandlerConfig)
	r, err := in.ReceiverSettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	receiver := r.(*proxyman.ReceiverConfig)
	receiver.SniffingSettings = &proxyman.SniffingConfig{Enabled: true, MetadataOnly: true, RouteOnly: true}
	in.ReceiverSettings = serial.ToTypedMessage(receiver)
	_, err = client.AlterInbound(ctx, &command.AlterInboundRequest{Tag: "hot", Operation: serial.ToTypedMessage(&command.UpdateInboundOperation{Inbound: in})})
	if err != nil {
		t.Fatal(err)
	}
	echo("after-hot-update")
	count, err := client.GetInboundUsersCount(ctx, &command.GetInboundUserRequest{Tag: "hot"})
	if err != nil || count.GetCount() != 1 {
		t.Fatalf("users lost: %v %v", count, err)
	}
	fresh, err := dialer.Dial("tcp", target.Addr().String())
	if err != nil {
		t.Fatal("existing credentials no longer authenticate:", err)
	}
	fresh.Close()
	// Listener change must fail before changing anything on the existing handler.
	receiver.PortList = proto.Clone(receiver.PortList).(*xnet.PortList)
	receiver.PortList.Range[0].From++
	in.ReceiverSettings = serial.ToTypedMessage(receiver)
	_, err = client.AlterInbound(ctx, &command.AlterInboundRequest{Tag: "hot", Operation: serial.ToTypedMessage(&command.UpdateInboundOperation{Inbound: in})})
	if err == nil {
		t.Fatal("accepted listener change as hot update")
	}
	echo("after-rejected-update")
}
