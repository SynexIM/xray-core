package dispatcher

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/stats"
)

func prepareConnectionStats(user *protocol.MemoryUser, manager stats.Manager, policies policy.Manager) error {
	if user == nil || user.Email == "" {
		return nil
	}
	registry, ok := manager.(stats.UserRegistry)
	if !ok {
		if isNoopStatsManager(manager) {
			return nil
		}
		return errors.New("stats.userRegistry: 统计管理器不支持身份句柄")
	}
	settings := policies.ForLevel(user.Level).Stats
	return user.PrepareStats(registry, settings.UserUplink, settings.UserDownlink, settings.UserOnline)
}
