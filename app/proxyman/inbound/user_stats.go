package inbound

import (
	"context"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/proxy"
)

func (handler *AlwaysOnInboundHandler) initializeUserStats(ctx context.Context) error {
	instance := core.MustFromContext(ctx)
	registry, ok := instance.GetFeature(stats.ManagerType()).(stats.UserRegistry)
	if !ok {
		return nil
	}
	handler.statsRegistry = registry
	handler.userPolicy = instance.GetFeature(policy.ManagerType()).(policy.Manager)
	if manager, ok := handler.proxy.(proxy.UserManager); ok {
		for _, user := range manager.GetUsers(ctx) {
			if err := handler.PrepareUserStats(user); err != nil {
				return err
			}
		}
	}
	return nil
}
func (handler *AlwaysOnInboundHandler) PrepareUserStats(user *protocol.MemoryUser) error {
	if handler.statsRegistry == nil {
		return nil
	}
	settings := handler.userPolicy.ForLevel(user.Level).Stats
	return user.PrepareStats(handler.statsRegistry, settings.UserUplink, settings.UserDownlink, settings.UserOnline)
}
func (handler *AlwaysOnInboundHandler) releaseUserStats() {
	if manager, ok := handler.proxy.(proxy.UserManager); ok {
		for _, user := range manager.GetUsers(context.Background()) {
			user.ReleaseStats()
		}
	}
}
