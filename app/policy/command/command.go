package command

import (
	"context"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/core"
	feature "github.com/xtls/xray-core/features/policy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type levelManager interface {
	SetLevels(map[uint32]*policy.Policy)
	GetLevels() map[uint32]*policy.Policy
}

type policyServer struct {
	UnimplementedPolicyServiceServer
	manager feature.Manager
}

func (s *policyServer) SetLevels(_ context.Context, request *SetLevelsRequest) (*SetLevelsResponse, error) {
	m, ok := s.manager.(levelManager)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "policy manager does not support level replacement")
	}
	m.SetLevels(request.GetLevels())
	return &SetLevelsResponse{}, nil
}

func (s *policyServer) GetLevels(context.Context, *GetLevelsRequest) (*GetLevelsResponse, error) {
	m, ok := s.manager.(levelManager)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "policy manager does not support level replacement")
	}
	return &GetLevelsResponse{Levels: m.GetLevels()}, nil
}

type service struct{ v *core.Instance }

func (s *service) Register(server *grpc.Server) {
	common.Must(s.v.RequireFeatures(func(m feature.Manager) {
		RegisterPolicyServiceServer(server, &policyServer{manager: m})
	}, false))
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, _ interface{}) (interface{}, error) {
		return &service{v: core.MustFromContext(ctx)}, nil
	}))
}
