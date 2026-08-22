package protocol

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
)

func (u *User) GetTypedAccount() (Account, error) {
	if u.GetAccount() == nil {
		return nil, errors.New("Account is missing").AtWarning()
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
	// 限速读的是 User 顶层字段，不是各协议自己的 account——所以每个协议
	// 都自动拿到同一套限速，不需要各自实现一个 limits 访问方法。
	// 运行时 AddUser 也只经过这一个入口，没有第二条路径能绕过去。
	return &MemoryUser{
		Account:             account,
		Email:               u.Email,
		Level:               u.Level,
		BandwidthBps:        u.BandwidthBps,
		ConnLimit:           u.ConnLimit,
		CommittedBps:        u.CommittedBps,
		CommittedBurstBytes: u.CommittedBurstBytes,
		Class:               u.Class,
	}, nil
}

func ToProtoUser(mu *MemoryUser) *User {
	if mu == nil {
		return nil
	}
	u := &User{
		Email:               mu.Email,
		Level:               mu.Level,
		BandwidthBps:        mu.BandwidthBps,
		ConnLimit:           mu.ConnLimit,
		CommittedBps:        mu.CommittedBps,
		CommittedBurstBytes: mu.CommittedBurstBytes,
		Class:               mu.Class,
	}
	// Account 可以没有：socks/http/mixed 这类静态入站会把用户表示成一个
	// 只带限速的 MemoryUser，密码另外放。序列化回去时必须容忍这一点，
	// 直接解引用会在这条路径上 panic。
	if mu.Account != nil {
		u.Account = serial.ToTypedMessage(mu.Account.ToProto())
	}
	return u
}

// MemoryUser is a parsed form of User, to reduce number of parsing of Account proto.
type MemoryUser struct {
	// Account is the parsed account of the protocol.
	Account      Account
	Email        string
	Level        uint32
	BandwidthBps uint64
	ConnLimit    uint32
	// 双速率（可选，见 user.proto 与 RuntimeRateLimiters）：
	// BandwidthBps 是 PIR，CommittedBps 是 CIR，CommittedBurstBytes 是 CBS。
	CommittedBps        uint64
	CommittedBurstBytes uint64

	// Class 是争抢等级名（= SKU）。策略表（weight / normal_cap / 突发信用）不在这里，
	// 走 NodeFairScheduler.SetClassPolicies 整份下发，见 node_fairshare.go。
	Class string
}
