// Package command 是 fork 的探测服务：经指定出站对一个 URL 发一次 HTTP GET。
//
// 与 observatory 用同一种拨号（tagged.Dialer 强制出站），但不依赖 observatory 段，
// 也不改它的状态：一次请求、一次结果，供控制面的 Probe 资源按需调用。
package command

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/xtls/xray-core/app/capability"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultTimeout = 10 * time.Second
	maxTimeout     = 30 * time.Second
	bodyLimit      = 256
)

type probeServer struct {
	UnimplementedProbeServiceServer
	ctx        context.Context
	dispatcher routing.Dispatcher
	ohm        outbound.Manager
}

func (s *probeServer) Probe(ctx context.Context, req *ProbeRequest) (*ProbeResponse, error) {
	if s.ohm.GetHandler(req.OutboundTag) == nil {
		return nil, status.Error(codes.NotFound, "outbound 不存在: "+req.OutboundTag)
	}
	target, err := url.Parse(req.Url)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return nil, status.Error(codes.InvalidArgument, "url 必须是 http(s) 地址")
	}
	timeout := defaultTimeout
	if req.TimeoutMs > 0 {
		timeout = min(time.Duration(req.TimeoutMs)*time.Millisecond, maxTimeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: func(*http.Request) (*url.URL, error) { return nil, nil },
			// 实例上下文里才有 Xray 的会话机制；超时由 Transport 在请求取消时关连接。
			DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
				dest, err := xnet.ParseDestination(network + ":" + addr)
				if err != nil {
					return nil, err
				}
				return tagged.Dialer(s.ctx, s.dispatcher, dest, req.OutboundTag)
			},
			DisableKeepAlives: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	start := time.Now()
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "经出站 "+req.OutboundTag+" 请求失败: "+err.Error())
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, bodyLimit))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "读取响应失败: "+err.Error())
	}
	return &ProbeResponse{StatusCode: uint32(resp.StatusCode), LatencyMs: uint64(time.Since(start).Milliseconds()), Body: body}, nil
}

// service 记下创建时的实例上下文：tagged.Dialer 要从里面取 core.Instance。
type service struct {
	v   *core.Instance
	ctx context.Context
}

func (s *service) Register(server *grpc.Server) {
	common.Must(s.v.RequireFeatures(func(d routing.Dispatcher, ohm outbound.Manager) {
		RegisterProbeServiceServer(server, &probeServer{ctx: s.ctx, dispatcher: d, ohm: ohm})
	}, false))
}

func init() {
	capability.Register("outbound.probe")
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, _ interface{}) (interface{}, error) {
		v := core.FromContext(ctx)
		if v == nil {
			return nil, errors.New("probe service 需要 Xray 实例")
		}
		return &service{v: v, ctx: core.ToBackgroundDetachedContext(ctx)}, nil
	}))
}
