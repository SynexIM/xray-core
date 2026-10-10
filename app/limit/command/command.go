package command

import (
	"context"
	"errors"

	"github.com/xtls/xray-core/app/limit"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/core"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type controller interface {
	SetNode(*limit.NodeLimit) error
	PutProfiles([]*limit.Profile) error
	RemoveProfiles([]string) error
	AddRules([]*limit.Rule) error
	RemoveRules([]string) error
	ListRules(uint32, uint32) ([]limit.RuleSummary, uint32)
	NodeStatus() limit.NodeStatus
	GetRuleStatus(string) (limit.RuleStatus, bool)
}
type limitServer struct {
	UnimplementedLimitServiceServer
	controller controller
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	var validation *limit.ValidationError
	if errors.As(err, &validation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	var inUse *limit.ProfileInUseError
	if errors.As(err, &inUse) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}
func (s *limitServer) SetNode(_ context.Context, request *SetNodeRequest) (*SetNodeResponse, error) {
	if err := s.controller.SetNode(request.Node); err != nil {
		return nil, rpcError(err)
	}
	return &SetNodeResponse{}, nil
}
func (s *limitServer) PutProfiles(_ context.Context, request *PutProfilesRequest) (*PutProfilesResponse, error) {
	if err := s.controller.PutProfiles(request.Profiles); err != nil {
		return nil, rpcError(err)
	}
	return &PutProfilesResponse{}, nil
}
func (s *limitServer) RemoveProfiles(_ context.Context, request *RemoveProfilesRequest) (*RemoveProfilesResponse, error) {
	if err := s.controller.RemoveProfiles(request.Names); err != nil {
		return nil, rpcError(err)
	}
	return &RemoveProfilesResponse{}, nil
}
func (s *limitServer) AddRules(_ context.Context, request *AddRulesRequest) (*AddRulesResponse, error) {
	if err := s.controller.AddRules(request.Rules); err != nil {
		return nil, rpcError(err)
	}
	return &AddRulesResponse{}, nil
}
func (s *limitServer) RemoveRules(_ context.Context, request *RemoveRulesRequest) (*RemoveRulesResponse, error) {
	if err := s.controller.RemoveRules(request.RuleTags); err != nil {
		return nil, rpcError(err)
	}
	return &RemoveRulesResponse{}, nil
}
func (s *limitServer) ListRules(_ context.Context, request *ListRulesRequest) (*ListRulesResponse, error) {
	rules, total := s.controller.ListRules(request.Offset, request.Limit)
	response := &ListRulesResponse{Total: total}
	for _, rule := range rules {
		response.Rules = append(response.Rules, &RuleSummary{RuleTag: rule.RuleTag, Profile: rule.Profile})
	}
	return response, nil
}
func directionStatus(s limit.DirectionStatus) *DirectionStatus {
	return &DirectionStatus{CurrentRateBps: s.RateBps, CurrentStep: s.Step, RemainingBytes: s.RemainingBytes, CapacityBytes: s.CapacityBytes}
}
func (s *limitServer) GetStatus(_ context.Context, request *GetStatusRequest) (*GetStatusResponse, error) {
	node := s.controller.NodeStatus()
	response := &GetStatusResponse{Node: &NodeStatus{Congested: node.Congested, UtilizationPercent: node.UtilizationPercent, UploadBps: node.UploadBps, DownloadBps: node.DownloadBps}}
	if request.RuleTag != "" {
		rule, ok := s.controller.GetRuleStatus(request.RuleTag)
		if !ok {
			return nil, status.Error(codes.NotFound, "limit rule 不存在: "+request.RuleTag)
		}
		response.Rule = &RuleStatus{RuleTag: rule.RuleTag, Profile: rule.Profile, Up: directionStatus(rule.Up), Down: directionStatus(rule.Down), Connections: rule.Connections, SourceIps: rule.SourceIPs}
	}
	return response, nil
}

type service struct{ v *core.Instance }

func (s *service) Register(server *grpc.Server) {
	common.Must(s.v.RequireFeatures(func(limiter featurelimit.Limiter) error {
		managed, ok := limiter.(controller)
		if !ok {
			return status.Error(codes.Unimplemented, "limit 实现不支持运行时管理")
		}
		RegisterLimitServiceServer(server, &limitServer{controller: managed})
		return nil
	}, false))
}
func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, _ interface{}) (interface{}, error) {
		return &service{v: core.MustFromContext(ctx)}, nil
	}))
}
