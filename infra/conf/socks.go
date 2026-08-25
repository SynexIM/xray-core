package conf

import (
	"encoding/json"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/socks"
	"google.golang.org/protobuf/proto"
)

type SocksAccount struct {
	Username     string `json:"user"`
	Password     string `json:"pass"`
	BandwidthBps uint64 `json:"bandwidth_bps"`
	ConnLimit    uint32 `json:"conn_limit"`
	// 双速率（可选）。committed_bps 是承诺速率 CIR，committed_burst_bytes 是
	// 突发额度 CBS（字节，留空 = 一天的承诺量）。语义见 protocol.User。
	CommittedBps        uint64 `json:"committed_bps"`
	CommittedBurstBytes uint64 `json:"committed_burst_bytes"`
	// class 标识共享同一争抢策略的客户组，策略表走 fairshare 的 SetClassPolicy 下发。
	Class string `json:"class"`
}

func (v *SocksAccount) Build() *socks.Account {
	return &socks.Account{
		Username: v.Username,
		Password: v.Password,
	}
}

const (
	AuthMethodNoAuth   = "noauth"
	AuthMethodUserPass = "password"
)

type SocksServerConfig struct {
	AuthMethod string          `json:"auth"`
	Users      []*SocksAccount `json:"users"`
	Accounts   []*SocksAccount `json:"accounts"`
	UDP        bool            `json:"udp"`
	Host       *Address        `json:"ip"`
	UserLevel  uint32          `json:"userLevel"`
}

func (v *SocksServerConfig) Build() (proto.Message, error) {
	config := new(socks.ServerConfig)
	switch v.AuthMethod {
	case AuthMethodNoAuth:
		config.AuthType = socks.AuthType_NO_AUTH
	case AuthMethodUserPass:
		config.AuthType = socks.AuthType_PASSWORD
	default:
		// errors.New("unknown socks auth method: ", v.AuthMethod, ". Default to noauth.").AtWarning().WriteToLog()
		config.AuthType = socks.AuthType_NO_AUTH
	}

	if v.Accounts != nil {
		v.Users = v.Accounts
	}
	// Build per-user accounts carrying protocol-agnostic limits (bandwidth +
	// connection caps), so a mixed/socks inbound enforces them for users baked in
	// at boot. The legacy `accounts` map (no limits) is left empty to avoid
	// double-registering the same usernames in the runtime user store.
	if len(v.Users) > 0 {
		config.UserAccounts = make([]*socks.UserAccount, 0, len(v.Users))
		for _, account := range v.Users {
			config.UserAccounts = append(config.UserAccounts, &socks.UserAccount{
				Username:            account.Username,
				Password:            account.Password,
				BandwidthBps:        account.BandwidthBps,
				ConnLimit:           account.ConnLimit,
				CommittedBps:        account.CommittedBps,
				CommittedBurstBytes: account.CommittedBurstBytes,
				Class:               account.Class,
			})
		}
	}

	config.UdpEnabled = v.UDP
	if v.Host != nil {
		config.Address = v.Host.Build()
	}

	config.UserLevel = v.UserLevel
	return config, nil
}

type SocksRemoteConfig struct {
	Address *Address          `json:"address"`
	Port    uint16            `json:"port"`
	Users   []json.RawMessage `json:"users"`
}

type SocksClientConfig struct {
	Address  *Address             `json:"address"`
	Port     uint16               `json:"port"`
	Level    uint32               `json:"level"`
	Email    string               `json:"email"`
	Username string               `json:"user"`
	Password string               `json:"pass"`
	Servers  []*SocksRemoteConfig `json:"servers"`
}

func (v *SocksClientConfig) Build() (proto.Message, error) {
	config := new(socks.ClientConfig)
	if v.Address != nil {
		v.Servers = []*SocksRemoteConfig{
			{
				Address: v.Address,
				Port:    v.Port,
			},
		}
		if len(v.Username) > 0 {
			v.Servers[0].Users = []json.RawMessage{{}}
		}
	}
	if len(v.Servers) != 1 {
		return nil, errors.New(`SOCKS settings: "servers" should have one and only one member. Multiple endpoints in "servers" should use multiple SOCKS outbounds and routing balancer instead`)
	}
	for _, serverConfig := range v.Servers {
		if len(serverConfig.Users) > 1 {
			return nil, errors.New(`SOCKS servers: "users" should have one member at most. Multiple members in "users" should use multiple SOCKS outbounds and routing balancer instead`)
		}
		server := &protocol.ServerEndpoint{
			Address: serverConfig.Address.Build(),
			Port:    uint32(serverConfig.Port),
		}
		for _, rawUser := range serverConfig.Users {
			user := new(protocol.User)
			if v.Address != nil {
				user.Level = v.Level
				user.Email = v.Email
			} else {
				if err := json.Unmarshal(rawUser, user); err != nil {
					return nil, errors.New("failed to parse Socks user").Base(err).AtError()
				}
			}
			account := new(SocksAccount)
			if v.Address != nil {
				account.Username = v.Username
				account.Password = v.Password
			} else {
				if err := json.Unmarshal(rawUser, account); err != nil {
					return nil, errors.New("failed to parse socks account").Base(err).AtError()
				}
			}
			user.Account = serial.ToTypedMessage(account.Build())
			server.User = user
			break
		}
		config.Server = server
		break
	}
	return config, nil
}
