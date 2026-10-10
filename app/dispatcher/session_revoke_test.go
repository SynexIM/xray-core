package dispatcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	handlercommand "github.com/xtls/xray-core/app/proxyman/command"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
	_ "github.com/xtls/xray-core/main/distro/all"
	xproxy "golang.org/x/net/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestKillAndRemoveDisconnectRealSocksSession(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		bytes := make([]byte, 1024)
		for {
			if _, err := w.Write(bytes); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	defer source.Close()
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := free.Addr().(*net.TCPAddr).Port
	free.Close()
	dir, err := os.MkdirTemp("", "xr-kill-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "api.sock")
	input := fmt.Sprintf(`{"inbounds":[{"tag":"entry","listen":"127.0.0.1","port":%d,"protocol":"socks","settings":{"auth":"password","accounts":[{"user":"login","pass":"test-only","email":"revoke-real"}]}}],"outbounds":[{"protocol":"freedom","settings":{}}],"api":{"tag":"api","listen":%q,"services":["HandlerService"]}}`, port, socket)
	var config conf.Config
	if err = json.Unmarshal([]byte(input), &config); err != nil {
		t.Fatal(err)
	}
	native, err := config.Build()
	if err != nil {
		t.Fatal(err)
	}
	instance, err := core.New(native)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	if err = instance.Start(); err != nil {
		t.Fatal(err)
	}
	dialer, err := xproxy.SOCKS5("tcp", fmt.Sprintf("127.0.0.1:%d", port), &xproxy.Auth{User: "login", Password: "test-only"}, &net.Dialer{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{DialContext: dialer.(xproxy.ContextDialer).DialContext, DisableKeepAlives: true}
	client := &http.Client{Transport: transport}
	defer client.CloseIdleConnections()
	open := func() (*http.Response, <-chan error) {
		t.Helper()
		response, err := client.Get(source.URL)
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{})
		done := make(chan error, 1)
		var once sync.Once
		go func() {
			defer response.Body.Close()
			buffer := make([]byte, 1024)
			for {
				n, err := response.Body.Read(buffer)
				if n > 0 {
					once.Do(func() { close(ready) })
				}
				if err != nil {
					done <- err
					return
				}
			}
		}()
		select {
		case <-ready:
		case <-time.After(time.Second):
			t.Fatal("真实代理流未启动")
		}
		return response, done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, "unix:"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	api := handlercommand.NewHandlerServiceClient(connection)
	first, closed := open()
	defer first.Body.Close()
	start := time.Now()
	if _, err = api.KillUserConnections(ctx, &handlercommand.KillUserConnectionsRequest{Emails: []string{"revoke-real"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("旧流没有结束错误")
		}
	case <-time.After(time.Second):
		t.Fatal("KillUserConnections 没在 1 秒内断开真实连接")
	}
	t.Logf("按 email 断开真实 SOCKS 流耗时 %s", time.Since(start))
	// 同凭据重连成功，证明 kill 没有删除账户。
	second, closedAgain := open()
	defer second.Body.Close()
	operation := serial.ToTypedMessage(&handlercommand.RemoveUserOperation{Email: "revoke-real"})
	if _, err = api.AlterInbound(ctx, &handlercommand.AlterInboundRequest{Tag: "entry", Operation: operation}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closedAgain:
	case <-time.After(time.Second):
		t.Fatal("RemoveUser 没在 1 秒内断旧连接")
	}
	if response, err := client.Get(source.URL); err == nil {
		response.Body.Close()
		t.Fatal("删除后旧凭据仍能认证")
	}
}
