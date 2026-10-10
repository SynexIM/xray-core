package command

import (
	"context"

	"github.com/xtls/xray-core/app/capability"
	"github.com/xtls/xray-core/common"
	"google.golang.org/grpc"
)

type service struct {
	UnimplementedCapabilityServiceServer
}

func (*service) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return &GetResponse{Version: capability.Version(), Capabilities: capability.List()}, nil
}

func (s *service) Register(server *grpc.Server) { RegisterCapabilityServiceServer(server, s) }

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(context.Context, interface{}) (interface{}, error) {
		return &service{}, nil
	}))
}
