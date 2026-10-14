package command

import (
	"context"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/features/inbound"
	"github.com/xtls/xray-core/proxy"
)

func prepareUserStats(handler inbound.Handler, user *protocol.MemoryUser) error {
	if initializer, ok := handler.(interface {
		PrepareUserStats(*protocol.MemoryUser) error
	}); ok {
		return initializer.PrepareUserStats(user)
	}
	return nil
}
func addUserWithStats(ctx context.Context, handler inbound.Handler, manager proxy.UserManager, user *protocol.MemoryUser) error {
	if err := prepareUserStats(handler, user); err != nil {
		return err
	}
	if err := manager.AddUser(ctx, user); err != nil {
		user.ReleaseStats()
		return err
	}
	return nil
}
func addUsersWithStats(ctx context.Context, handler inbound.Handler, manager proxy.UserManager, users []*protocol.MemoryUser) error {
	for index, user := range users {
		if err := prepareUserStats(handler, user); err != nil {
			for _, prepared := range users[:index] {
				prepared.ReleaseStats()
			}
			return err
		}
	}
	if batch, ok := manager.(proxy.BatchUserManager); ok {
		if err := batch.AddUsers(ctx, users); err != nil {
			for _, user := range users {
				user.ReleaseStats()
			}
			return err
		}
		return nil
	}
	for index, user := range users {
		if err := manager.AddUser(ctx, user); err != nil {
			for _, pending := range users[index:] {
				pending.ReleaseStats()
			}
			return err
		}
	}
	return nil
}
