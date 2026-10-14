package vmess

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"strings"
)

func (validator *TimedUserValidator) AddUsers(users []*protocol.MemoryUser) error {
	validator.Lock()
	defer validator.Unlock()
	if len(validator.users)+len(users) > MaxUsersPerInbound {
		return &UserCapacityError{Maximum: MaxUsersPerInbound}
	}
	seen := make(map[string]bool, len(users))
	for _, user := range users {
		if _, ok := user.Account.(*MemoryAccount); !ok {
			return errors.New("account type is incorrect")
		}
		if user.Email == "" {
			continue
		}
		email := strings.ToLower(user.Email)
		if _, exists := validator.index[email]; exists || seen[email] {
			return errors.New("User ", user.Email, " already exists.")
		}
		seen[email] = true
	}
	for _, user := range users {
		validator.addLocked(user, user.Account.(*MemoryAccount))
	}
	return nil
}
func (validator *TimedUserValidator) RemoveUsers(emails []string) error {
	validator.Lock()
	defer validator.Unlock()
	for _, email := range emails {
		if _, exists := validator.index[strings.ToLower(email)]; !exists {
			return errors.New("User ", email, " not found.")
		}
	}
	for _, email := range emails {
		validator.removeLocked(strings.ToLower(email))
	}
	return nil
}
