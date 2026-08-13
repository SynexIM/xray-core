package command

import (
	"context"

	"github.com/xtls/xray-core/common"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// puller 是聚合器拉取接口（避免本包 import app/accesslog 造成环依赖：
// 聚合器 import 本包取 proto 类型，本包仅持一个由 app/accesslog Init 注入的回调）。
var puller func(reset bool, topN int) *AccessReport

// SetPuller 由 app/accesslog Init 注入聚合器的 Pull 入口。
func SetPuller(fn func(reset bool, topN int) *AccessReport) { puller = fn }

// accessLogServer 实现 AccessLogService：经注入的 puller 取窗口。
type accessLogServer struct{}

func NewAccessLogServer() AccessLogServiceServer { return &accessLogServer{} }

func (s *accessLogServer) PullAccess(ctx context.Context, req *PullAccessRequest) (*AccessReport, error) {
	if puller == nil {
		return nil, status.Error(codes.Unavailable, "accesslog app not configured")
	}
	return puller(req.GetReset_(), int(req.GetTopN())), nil
}

func (s *accessLogServer) mustEmbedUnimplementedAccessLogServiceServer() {}

type service struct{}

func (s *service) Register(server *grpc.Server) {
	RegisterAccessLogServiceServer(server, NewAccessLogServer())
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return new(service), nil
	}))
}
