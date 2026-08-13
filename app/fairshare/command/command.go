package command

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/protocol"
	grpc "google.golang.org/grpc"
)

// fairShareServer 实现 FairShareService：把节点总出口上限喂进程内 NodeFairScheduler 单例。
type fairShareServer struct{}

func NewFairShareServer() FairShareServiceServer { return &fairShareServer{} }

func (s *fairShareServer) SetNodeBandwidth(ctx context.Context, req *SetNodeBandwidthRequest) (*SetNodeBandwidthResponse, error) {
	// 地板先于总额生效，避免开启瞬间用默认地板重算一轮。0=默认（旧 node-agent 不带
	// 该字段 → 向后兼容默认 0.5Mbps 软地板 / 16KB/s 硬地板）。
	protocol.FairScheduler().SetFloors(req.GetSoftFloorBps(), req.GetHardFloorBps())
	protocol.FairScheduler().SetNodeBandwidth(req.GetAvailBps())
	return &SetNodeBandwidthResponse{}, nil
}

func (s *fairShareServer) mustEmbedUnimplementedFairShareServiceServer() {}

type service struct{}

func (s *service) Register(server *grpc.Server) {
	RegisterFairShareServiceServer(server, NewFairShareServer())
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		return new(service), nil
	}))
}
