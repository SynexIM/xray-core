package command

import (
	"context"
	"slices"
	"time"

	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
	"google.golang.org/grpc"
)

// routingServer is an implementation of RoutingService.
type routingServer struct {
	router       routing.Router
	routingStats stats.Channel
}

func (s *routingServer) GetBalancerInfo(ctx context.Context, request *GetBalancerInfoRequest) (*GetBalancerInfoResponse, error) {
	var ret GetBalancerInfoResponse
	ret.Balancer = &BalancerMsg{}
	if bo, ok := s.router.(routing.BalancerOverrider); ok {
		{
			res, err := bo.GetOverrideTarget(request.GetTag())
			if err != nil {
				return nil, err
			}
			ret.Balancer.Override = &OverrideInfo{
				Target: res,
			}
		}
	}

	if pt, ok := s.router.(routing.BalancerPrincipleTarget); ok {
		{
			res, err := pt.GetPrincipleTarget(request.GetTag())
			if err != nil {
				errors.LogInfoInner(ctx, err, "unable to obtain principle target")
			} else {
				ret.Balancer.PrincipleTarget = &PrincipleTargetInfo{Tag: res}
			}
		}
	}
	return &ret, nil
}

func (s *routingServer) OverrideBalancerTarget(ctx context.Context, request *OverrideBalancerTargetRequest) (*OverrideBalancerTargetResponse, error) {
	if bo, ok := s.router.(routing.BalancerOverrider); ok {
		return &OverrideBalancerTargetResponse{}, bo.SetOverrideTarget(request.BalancerTag, request.Target)
	}
	return nil, errors.New("unsupported router implementation")
}

func (s *routingServer) AddRule(ctx context.Context, request *AddRuleRequest) (*AddRuleResponse, error) {
	if request.ReplaceExisting {
		if writer, ok := s.router.(interface {
			ReplaceRule(*serial.TypedMessage) error
		}); ok {
			return &AddRuleResponse{}, writer.ReplaceRule(request.Config)
		}
		return nil, errors.New("router cannot atomically replace a rule")
	}
	if bo, ok := s.router.(routing.Router); ok {
		return &AddRuleResponse{}, bo.AddRule(request.Config, request.ShouldAppend)
	}
	return nil, errors.New("unsupported router implementation")
}

func (s *routingServer) RemoveRule(ctx context.Context, request *RemoveRuleRequest) (*RemoveRuleResponse, error) {
	if bo, ok := s.router.(routing.Router); ok {
		return &RemoveRuleResponse{}, bo.RemoveRule(request.RuleTag)
	}
	return nil, errors.New("unsupported router implementation")
}

func (s *routingServer) ListRule(ctx context.Context, request *ListRuleRequest) (*ListRuleResponse, error) {
	if bo, ok := s.router.(routing.Router); ok {
		response := &ListRuleResponse{}
		for _, v := range bo.ListRule() {
			response.Rules = append(response.Rules, &ListRuleItem{
				Tag:     v.GetOutboundTag(),
				RuleTag: v.GetRuleTag(),
			})
		}
		return response, nil
	}
	return nil, errors.New("unsupported router implementation")
}

// BatchAddRule appends/inserts many rules over a single gRPC call. Rules apply in
// request order; per-rule failures are reported in the response rather than
// aborting the batch, so the caller retries only the failed slots. This keeps
// per-client route churn scaling with node requests, not per-rule round trips.
func (s *routingServer) BatchAddRule(ctx context.Context, request *BatchAddRuleRequest) (*BatchRuleResponse, error) {
	bo, ok := s.router.(routing.Router)
	if !ok {
		return nil, errors.New("unsupported router implementation")
	}
	response := &BatchRuleResponse{
		Results: make([]*BatchRuleOperationResult, 0, len(request.Configs)),
	}
	// Prefer the router's own batch path (one lock, one snapshot, O(N)); fall back
	// to per-rule AddRule for routers that do not provide it.
	var batchErrs []error
	if br, ok := s.router.(interface {
		AddRules([]*serial.TypedMessage, bool) []error
	}); ok {
		batchErrs = br.AddRules(request.Configs, request.ShouldAppend)
	}
	for i, config := range request.Configs {
		result := &BatchRuleOperationResult{Index: uint32(i)}
		var err error
		if batchErrs != nil {
			err = batchErrs[i]
		} else {
			err = bo.AddRule(config, request.ShouldAppend)
		}
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Success = true
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

// BatchRemoveRule removes many rules by tag over a single gRPC call, with the same
// per-operation result reporting as BatchAddRule.
func (s *routingServer) BatchRemoveRule(ctx context.Context, request *BatchRemoveRuleRequest) (*BatchRuleResponse, error) {
	bo, ok := s.router.(routing.Router)
	if !ok {
		return nil, errors.New("unsupported router implementation")
	}
	response := &BatchRuleResponse{
		Results: make([]*BatchRuleOperationResult, 0, len(request.RuleTags)),
	}
	var batchErrs []error
	if batch, ok := s.router.(interface{ RemoveRules([]string) []error }); ok {
		batchErrs = batch.RemoveRules(request.RuleTags)
	}
	for i, tag := range request.RuleTags {
		result := &BatchRuleOperationResult{Index: uint32(i)}
		var err error
		if batchErrs != nil {
			err = batchErrs[i]
		} else {
			err = bo.RemoveRule(tag)
		}
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Success = true
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

// ListRuleFull returns the full per-rule routing data (rule tag, user/email
// condition, target outbound, inbound) needed for snapshot/drift detection by the
// backend reconcile path. It reads from the same ListRule() the router already
// exposes; no router implementation change is required.
func (s *routingServer) ListRuleFull(ctx context.Context, request *ListRuleFullRequest) (*ListRuleFullResponse, error) {
	bo, ok := s.router.(routing.Router)
	if !ok {
		return nil, errors.New("unsupported router implementation")
	}
	response := &ListRuleFullResponse{}
	var configs map[string][]byte
	if reader, ok := s.router.(interface{ RuleConfigByTag() map[string][]byte }); ok {
		configs = reader.RuleConfigByTag()
	}
	for _, v := range bo.ListRule() {
		if len(request.Tags) > 0 && !slices.Contains(request.Tags, v.GetRuleTag()) {
			continue
		}
		response.Rules = append(response.Rules, &FullRuleItem{
			RuleTag:      v.GetRuleTag(),
			OutboundTag:  v.GetOutboundTag(),
			User:         v.GetUser(),
			InboundTag:   v.GetInboundTag(),
			NativeConfig: configs[v.GetRuleTag()],
		})
	}
	return response, nil
}

type balancerWriter interface {
	PutBalancers([]*router.BalancingRule) error
	RemoveBalancers([]string) error
}

func (s *routingServer) PutBalancers(ctx context.Context, request *PutBalancersRequest) (*PutBalancersResponse, error) {
	if w, ok := s.router.(balancerWriter); ok {
		return &PutBalancersResponse{}, w.PutBalancers(request.Balancers)
	}
	return nil, errors.New("router cannot update balancers")
}

func (s *routingServer) RemoveBalancers(ctx context.Context, request *RemoveBalancersRequest) (*RemoveBalancersResponse, error) {
	if w, ok := s.router.(balancerWriter); ok {
		return &RemoveBalancersResponse{}, w.RemoveBalancers(request.Tags)
	}
	return nil, errors.New("router cannot update balancers")
}

// NewRoutingServer creates a statistics service with statistics manager.
func NewRoutingServer(router routing.Router, routingStats stats.Channel) RoutingServiceServer {
	return &routingServer{
		router:       router,
		routingStats: routingStats,
	}
}

func (s *routingServer) TestRoute(ctx context.Context, request *TestRouteRequest) (*RoutingContext, error) {
	if request.RoutingContext == nil {
		return nil, errors.New("Invalid routing request.")
	}
	route, err := s.router.PickRoute(AsRoutingContext(request.RoutingContext))
	if err != nil {
		return nil, err
	}
	if request.PublishResult && s.routingStats != nil {
		ctx, _ := context.WithTimeout(context.Background(), 4*time.Second)
		s.routingStats.Publish(ctx, route)
	}
	return AsProtobufMessage(request.FieldSelectors)(route), nil
}

func (s *routingServer) SubscribeRoutingStats(request *SubscribeRoutingStatsRequest, stream RoutingService_SubscribeRoutingStatsServer) error {
	if s.routingStats == nil {
		return errors.New("Routing statistics not enabled.")
	}
	genMessage := AsProtobufMessage(request.FieldSelectors)
	subscriber, err := stats.SubscribeRunnableChannel(s.routingStats)
	if err != nil {
		return err
	}
	defer stats.UnsubscribeClosableChannel(s.routingStats, subscriber)
	for {
		select {
		case value, ok := <-subscriber:
			if !ok {
				return errors.New("Upstream closed the subscriber channel.")
			}
			route, ok := value.(routing.Route)
			if !ok {
				return errors.New("Upstream sent malformed statistics.")
			}
			err := stream.Send(genMessage(route))
			if err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *routingServer) mustEmbedUnimplementedRoutingServiceServer() {}

type service struct {
	v *core.Instance
}

func (s *service) Register(server *grpc.Server) {
	common.Must(s.v.RequireFeatures(func(router routing.Router, stats stats.Manager) {
		rs := NewRoutingServer(router, nil)
		RegisterRoutingServiceServer(server, rs)

		// For compatibility purposes
		vCoreDesc := RoutingService_ServiceDesc
		vCoreDesc.ServiceName = "v2ray.core.app.router.command.RoutingService"
		server.RegisterService(&vCoreDesc, rs)
	}, false))
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		s := core.MustFromContext(ctx)
		return &service{v: s}, nil
	}))
}
