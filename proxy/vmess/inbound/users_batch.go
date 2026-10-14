package inbound

import (
	"context"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"strings"
)

func (handler *Handler) AddUsers(_ context.Context, users []*protocol.MemoryUser) error {
	handler.usersByEmail.Lock()
	defer handler.usersByEmail.Unlock()
	for _, user := range users {
		if user.Email != "" {
			if handler.usersByEmail.cache[strings.ToLower(user.Email)] != nil {
				return errors.New("User ", user.Email, " already exists.")
			}
		}
	}
	if err := handler.clients.AddUsers(users); err != nil {
		return err
	}
	for _, user := range users {
		if user.Email != "" {
			handler.usersByEmail.cache[strings.ToLower(user.Email)] = user
		}
	}
	return nil
}
func (handler *Handler) RemoveUsers(_ context.Context, emails []string) error {
	handler.usersByEmail.Lock()
	stale := make([]*protocol.MemoryUser, 0, len(emails))
	for _, email := range emails {
		user := handler.usersByEmail.cache[strings.ToLower(email)]
		if email == "" || user == nil {
			handler.usersByEmail.Unlock()
			return errors.New("User ", email, " not found.")
		}
		stale = append(stale, user)
	}
	if err := handler.clients.RemoveUsers(emails); err != nil {
		handler.usersByEmail.Unlock()
		return err
	}
	for _, email := range emails {
		delete(handler.usersByEmail.cache, strings.ToLower(email))
	}
	handler.usersByEmail.Unlock()
	for _, user := range stale {
		user.RevokeRuntimeSessions()
	}
	return nil
}
