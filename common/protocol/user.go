package protocol

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"sync/atomic"
)

func (u *User) GetTypedAccount() (Account, error) {
	if u.GetAccount() == nil {
		return nil, errors.New("Account is missing")
	}

	rawAccount, err := u.Account.GetInstance()
	if err != nil {
		return nil, err
	}
	if asAccount, ok := rawAccount.(AsAccount); ok {
		return asAccount.AsAccount()
	}
	if account, ok := rawAccount.(Account); ok {
		return account, nil
	}
	return nil, errors.New("Unknown account type: ", u.Account.Type)
}

func (u *User) ToMemoryUser() (*MemoryUser, error) {
	account, err := u.GetTypedAccount()
	if err != nil {
		return nil, err
	}
	mu := u.RuntimeMemoryUser()
	mu.Account = account
	return mu, nil
}

// 静态 HTTP/SOCKS 凭据由入站管理，只在此复制身份。
func (u *User) RuntimeMemoryUser() *MemoryUser {
	return &MemoryUser{
		Email: u.Email,
		Level: u.Level,
	}
}

func ToProtoUser(mu *MemoryUser) *User {
	if mu == nil {
		return nil
	}
	account := mu.Account
	u := &User{
		Email: mu.Email,
		Level: mu.Level,
	}
	// 启动身份的凭据可能由入站单独管理。
	if account != nil {
		u.Account = serial.ToTypedMessage(account.ToProto())
	}
	return u
}

// MemoryUser is a parsed form of User, to reduce number of parsing of Account proto.
type MemoryUser struct {
	runtimeStats    atomic.Pointer[userStats]
	runtimeSessions atomic.Pointer[userSessions]
	Account         Account
	Email           string
	Level           uint32
}
