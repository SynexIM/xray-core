package command_test

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	_ "github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/app/reverse"
	. "github.com/xtls/xray-core/app/reverse/command"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// 起一个真的 xray 实例（带 reverse app），把 ReverseService 挂在 bufconn 上，
// 然后**只通过 gRPC** 改配置。这样测的是 API 调用方真正会走的那条路，
// 而不是直接调 Go 方法自欺欺人。
func newTestClient(t *testing.T, bridges []*reverse.BridgeConfig, portals []*reverse.PortalConfig) (ReverseServiceClient, *core.Instance) {
	t.Helper()

	v, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
			serial.ToTypedMessage(&reverse.Config{
				BridgeConfig: bridges,
				PortalConfig: portals,
			}),
		},
	})
	if err != nil {
		t.Fatalf("起 xray 实例失败：%v", err)
	}
	if err := v.Start(); err != nil {
		t.Fatalf("启动 xray 实例失败：%v", err)
	}
	t.Cleanup(func() { v.Close() })

	svc, err := core.CreateObject(v, &Config{})
	if err != nil {
		t.Fatalf("建 ReverseService 失败：%v", err)
	}
	registrar, ok := svc.(interface{ Register(*grpc.Server) })
	if !ok {
		t.Fatalf("ReverseService 不是一个 gRPC service：%T", svc)
	}

	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	registrar.Register(server)
	go server.Serve(lis)
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("连 bufconn 失败：%v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return NewReverseServiceClient(conn), v
}

func listTags(t *testing.T, c ReverseServiceClient) ([]string, []string) {
	t.Helper()
	resp, err := c.ListReverse(context.Background(), &ListReverseRequest{})
	if err != nil {
		t.Fatalf("ListReverse 失败：%v", err)
	}
	var bridges, portals []string
	for _, b := range resp.GetBridges() {
		bridges = append(bridges, b.GetTag())
	}
	for _, p := range resp.GetPortals() {
		portals = append(portals, p.GetTag())
	}
	return bridges, portals
}

// 热改 bridge：加、查、重复加要报错、删、删不存在的要报错。
func TestHotReconfigureBridge(t *testing.T) {
	c, _ := newTestClient(t, nil, nil)

	if b, _ := listTags(t, c); len(b) != 0 {
		t.Fatalf("初始应该没有 bridge，实际 %v", b)
	}

	_, err := c.AddBridge(context.Background(), &AddBridgeRequest{
		Bridge: &reverse.BridgeConfig{Tag: "bridge-a", Domain: "a.reverse.internal"},
	})
	common.Must(err)

	bridges, _ := listTags(t, c)
	if len(bridges) != 1 || bridges[0] != "bridge-a" {
		t.Fatalf("热加后没看到 bridge-a，实际 %v", bridges)
	}

	if _, err := c.AddBridge(context.Background(), &AddBridgeRequest{
		Bridge: &reverse.BridgeConfig{Tag: "bridge-a", Domain: "other.internal"},
	}); err == nil {
		t.Error("重复 tag 必须报错——静默覆盖会让路由指向哪一个全靠运气")
	}

	_, err = c.RemoveBridge(context.Background(), &RemoveBridgeRequest{Tag: "bridge-a"})
	common.Must(err)
	if bridges, _ := listTags(t, c); len(bridges) != 0 {
		t.Fatalf("删完还在：%v", bridges)
	}

	if _, err := c.RemoveBridge(context.Background(), &RemoveBridgeRequest{Tag: "ghost"}); err == nil {
		t.Error("删不存在的 bridge 必须报错，不能假装成功")
	}
}

// 热改 portal：加了要真的注册进 outbound manager（不然流量根本不会走过去），
// 删了要真的摘掉。
func TestHotReconfigurePortalRegistersOutbound(t *testing.T) {
	c, v := newTestClient(t, nil, nil)
	ohm := v.GetFeature(outbound.ManagerType()).(outbound.Manager)

	_, err := c.AddPortal(context.Background(), &AddPortalRequest{
		Portal: &reverse.PortalConfig{Tag: "portal-a", Domain: "a.reverse.internal"},
	})
	common.Must(err)

	if _, portals := listTags(t, c); len(portals) != 1 || portals[0] != "portal-a" {
		t.Fatalf("热加后没看到 portal-a，实际 %v", portals)
	}
	if ohm.GetHandler("portal-a") == nil {
		t.Fatal("portal 加进去了却没注册 outbound handler——配置改了但流量走不过去")
	}

	_, err = c.RemovePortal(context.Background(), &RemovePortalRequest{Tag: "portal-a"})
	common.Must(err)
	if ohm.GetHandler("portal-a") != nil {
		t.Fatal("portal 删了但 outbound handler 还在")
	}
}

// 这条才是热改的真正卖点：改一个客户的入口，**不能**动到同节点上其他客户。
// 重启进程做不到这一点，所以这里显式盯住它。
func TestHotReconfigureLeavesOthersAlone(t *testing.T) {
	c, v := newTestClient(t, nil, []*reverse.PortalConfig{
		{Tag: "portal-keep", Domain: "keep.internal"},
	})
	ohm := v.GetFeature(outbound.ManagerType()).(outbound.Manager)

	_, err := c.AddPortal(context.Background(), &AddPortalRequest{
		Portal: &reverse.PortalConfig{Tag: "portal-churn", Domain: "churn.internal"},
	})
	common.Must(err)
	_, err = c.RemovePortal(context.Background(), &RemovePortalRequest{Tag: "portal-churn"})
	common.Must(err)

	_, portals := listTags(t, c)
	if len(portals) != 1 || portals[0] != "portal-keep" {
		t.Fatalf("动了别人的 portal：%v", portals)
	}
	if ohm.GetHandler("portal-keep") == nil {
		t.Fatal("另一个客户的 outbound handler 被连累摘掉了")
	}
}

// 启动时就配好的 bridge/portal 要能被列出来——API 调用方拿 List 当「现在到底是什么」
// 的唯一事实来源，漏掉启动配置会让它以为节点是空的，进而重复下发。
func TestListReflectsBootConfig(t *testing.T) {
	c, _ := newTestClient(t,
		[]*reverse.BridgeConfig{{Tag: "boot-bridge", Domain: "b.internal"}},
		[]*reverse.PortalConfig{{Tag: "boot-portal", Domain: "p.internal"}})

	bridges, portals := listTags(t, c)
	if len(bridges) != 1 || bridges[0] != "boot-bridge" {
		t.Errorf("启动配置里的 bridge 没被列出来：%v", bridges)
	}
	if len(portals) != 1 || portals[0] != "boot-portal" {
		t.Errorf("启动配置里的 portal 没被列出来：%v", portals)
	}
}

// reverse 没启用时要给一句人能看懂的错，而不是空指针或者永远挂着。
func TestReverseNotEnabled(t *testing.T) {
	v, err := core.New(&core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&dispatcher.Config{}),
			serial.ToTypedMessage(&proxyman.InboundConfig{}),
			serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		},
	})
	common.Must(err)
	common.Must(v.Start())
	defer v.Close()

	svc, err := core.CreateObject(v, &Config{})
	common.Must(err)
	registrar := svc.(interface{ Register(*grpc.Server) })

	lis := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	registrar.Register(server)
	go server.Serve(lis)
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	common.Must(err)
	defer conn.Close()

	if _, err := NewReverseServiceClient(conn).ListReverse(context.Background(), &ListReverseRequest{}); err == nil {
		t.Error("节点没启用 reverse，调用必须报错")
	}
}
