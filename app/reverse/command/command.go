package command

import (
	"context"

	"github.com/xtls/xray-core/app/reverse"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	grpc "google.golang.org/grpc"
)

// reverseServer 实现 ReverseService：把 bridge/portal 的增删查接到进程内的
// *reverse.Reverse 上，让 API 调用方换入口时不必重启 xray。
type reverseServer struct {
	v *core.Instance
}

// instance 每次调用时现取，而不是在 Register 时锁定。
//
// 理由：reverse 是可选 app。用 RequireFeatures 在注册时等它，节点没配 reverse
// 时那个回调会一直挂着；现取则能立刻返回一句人能看懂的错误。
func (s *reverseServer) instance() (*reverse.Reverse, error) {
	f := s.v.GetFeature((*reverse.Reverse)(nil))
	if f == nil {
		return nil, errors.New("reverse is not enabled on this node")
	}
	r, ok := f.(*reverse.Reverse)
	if !ok {
		return nil, errors.New("unexpected reverse feature type")
	}
	return r, nil
}

func (s *reverseServer) AddBridge(ctx context.Context, request *AddBridgeRequest) (*AddBridgeResponse, error) {
	r, err := s.instance()
	if err != nil {
		return nil, err
	}
	if err := r.AddBridge(request.GetBridge()); err != nil {
		return nil, err
	}
	return &AddBridgeResponse{}, nil
}

func (s *reverseServer) RemoveBridge(ctx context.Context, request *RemoveBridgeRequest) (*RemoveBridgeResponse, error) {
	r, err := s.instance()
	if err != nil {
		return nil, err
	}
	if err := r.RemoveBridge(request.GetTag()); err != nil {
		return nil, err
	}
	return &RemoveBridgeResponse{}, nil
}

func (s *reverseServer) AddPortal(ctx context.Context, request *AddPortalRequest) (*AddPortalResponse, error) {
	r, err := s.instance()
	if err != nil {
		return nil, err
	}
	if err := r.AddPortal(request.GetPortal()); err != nil {
		return nil, err
	}
	return &AddPortalResponse{}, nil
}

func (s *reverseServer) RemovePortal(ctx context.Context, request *RemovePortalRequest) (*RemovePortalResponse, error) {
	r, err := s.instance()
	if err != nil {
		return nil, err
	}
	if err := r.RemovePortal(request.GetTag()); err != nil {
		return nil, err
	}
	return &RemovePortalResponse{}, nil
}

func (s *reverseServer) ListReverse(ctx context.Context, request *ListReverseRequest) (*ListReverseResponse, error) {
	r, err := s.instance()
	if err != nil {
		return nil, err
	}
	bridges, portals := r.List()
	return &ListReverseResponse{Bridges: bridges, Portals: portals}, nil
}

func (s *reverseServer) mustEmbedUnimplementedReverseServiceServer() {}

type service struct {
	v *core.Instance
}

func (s *service) Register(server *grpc.Server) {
	RegisterReverseServiceServer(server, &reverseServer{v: s.v})
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return &service{v: core.MustFromContext(ctx)}, nil
	}))
}
