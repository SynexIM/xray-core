package command

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/protocol"
	grpc "google.golang.org/grpc"
)

// fairShareServer 实现 FairShareService：把节点级调度参数喂进程内 NodeFairScheduler 单例。
//
// ⚠️ 单位：本包收到的 avail_bps / soft_floor_bps / hard_floor_bps 都是**字节/秒**，
// 原样进调度器，一次都不除 8。除以 8 的地方只有一处，在 common/protocol/user_limits.go，
// 那里换的是 User 的比特/秒。两边由 command_units_test.go 钉死。
type fairShareServer struct{}

func NewFairShareServer() FairShareServiceServer { return &fairShareServer{} }

func (s *fairShareServer) SetNodeBandwidth(ctx context.Context, req *SetNodeBandwidthRequest) (*SetNodeBandwidthResponse, error) {
	// 地板与滞回先于总额生效，避免开启的那一瞬间用旧参数白算一轮。
	// 0 一律是「不启用」，不是「用默认值」（FR-079c：不许有默认带宽）。
	sched := protocol.FairScheduler()
	sched.SetFloors(req.GetSoftFloorBps(), req.GetHardFloorBps())
	sched.SetCongestionHysteresis(req.GetCongestionEnterPercent(), req.GetCongestionExitPercent(), req.GetCongestionExitTicks())
	sched.SetNodeBandwidth(req.GetAvailBps())
	return &SetNodeBandwidthResponse{}, nil
}

func (s *fairShareServer) SetClassPolicy(ctx context.Context, req *SetClassPolicyRequest) (*SetClassPolicyResponse, error) {
	protocol.FairScheduler().SetClassPolicies(toClassPolicies(req.GetClasses()))
	return &SetClassPolicyResponse{}, nil
}

// toClassPolicies 把线上消息翻成进程内策略。字段名带单位后缀，这里就不会译错。
func toClassPolicies(in []*ClassPolicy) []*protocol.ClassPolicy {
	out := make([]*protocol.ClassPolicy, 0, len(in))
	for _, c := range in {
		if c == nil {
			continue
		}
		out = append(out, &protocol.ClassPolicy{
			Name:                c.GetName(),
			Weight:              c.GetWeight(),
			NormalCapBytePerSec: c.GetNormalCapBytePerSec(),
			BurstCapBytePerSec:  c.GetBurstCapBytePerSec(),
			BurstCreditBytes:    c.GetBurstCreditBytes(),
			FloorRatioPercent:   c.GetFloorRatioPercent(),
		})
	}
	return out
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
