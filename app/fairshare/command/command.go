package command

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/protocol"
	grpc "google.golang.org/grpc"
)

// fairShareServer 实现 FairShareService：把节点级调度参数喂进程内 NodeFairScheduler 单例。
// 本包的速率都是字节/秒，原样进调度器；0 一律是「没有这一项」。
type fairShareServer struct{}

func NewFairShareServer() FairShareServiceServer { return &fairShareServer{} }

func (s *fairShareServer) SetNodeBandwidth(ctx context.Context, req *SetNodeBandwidthRequest) (*SetNodeBandwidthResponse, error) {
	// 滞回先于总额生效，避免开启的那一瞬间用旧参数白算一轮。
	sched := protocol.FairScheduler()
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
			Name:                       c.GetName(),
			Weight:                     c.GetWeight(),
			FloorBytePerSec:            c.GetFloorBytePerSec(),
			UploadReservedBytePerSec:   c.GetUploadReservedBytePerSec(),
			DownloadReservedBytePerSec: c.GetDownloadReservedBytePerSec(),
			HeavyWindowSeconds:         c.GetHeavyWindowSeconds(),
			HeavyPercent:               c.GetHeavyPercent(),
		})
	}
	return out
}

// GetStatus 把调度器运行态原样报出去。只读，不抢 recompute 的锁。
func (s *fairShareServer) GetStatus(ctx context.Context, req *GetStatusRequest) (*GetStatusResponse, error) {
	st := protocol.FairScheduler().Status()
	return &GetStatusResponse{
		RootCapBytePerSec:       st.RootCapBytePerSec,
		Congested:               st.Congested,
		ActiveMembers:           st.ActiveMembers,
		FillTruncated:           st.FillTruncated,
		FillUnresolvedMembers:   st.FillUnresolved,
		FillTruncatedTicks:      st.FillTruncatedTicks,
		FillTruncatedTotalTicks: st.FillTruncatedTotal,
		FillRounds:              st.FillRounds,
		HeavyMembers:            st.HeavyMembers,
		UsedUploadBytePerSec:    st.UsedUploadBytePerSec,
		UsedDownloadBytePerSec:  st.UsedDownloadBytePerSec,
	}, nil
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
