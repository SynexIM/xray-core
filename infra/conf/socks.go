package conf

import (
	"encoding/json"
	"reflect"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/socks"
	"google.golang.org/protobuf/proto"
)

type SocksAccount struct {
	Username string `json:"user"`
	Password string `json:"pass"`
	// Email 用于规则匹配和会话管理。
	Email string `json:"email"`
}

func (*SocksServerConfig) strictAccountType() reflect.Type { return reflect.TypeFor[SocksAccount]() }
func (*SocksClientConfig) strictAccountType() reflect.Type { return reflect.TypeFor[SocksAccount]() }

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
		config.AuthType = socks.AuthType_NO_AUTH
	}

	if v.Accounts != nil {
		v.Users = v.Accounts
	}
	if len(v.Users) > 0 {
		config.UserAccounts = make([]*socks.UserAccount, 0, len(v.Users))
		for _, account := range v.Users {
			config.UserAccounts = append(config.UserAccounts, &socks.UserAccount{
				Username: account.Username,
				Password: account.Password,
				Runtime:  account.runtimeUser(),
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
	// 以下三项是 fork 的性能优化，默认开、上游不支持自动不用，写 false 才关（proxy/socks/upstream.go）。
	// Pipeline：握手各段错开几毫秒发出、不等回复，后台自检通过才用。
	Pipeline *bool `json:"pipeline"`
	// Prewarm：按需求预先建好已认证的上游连接，省掉 TCP 握手与认证。
	Prewarm *bool `json:"prewarm"`
	// EarlyData：首包紧跟请求发出、不等"连接成功"，后台自检通过才用。
	EarlyData *bool `json:"earlyData"`
}

func (v *SocksClientConfig) Build() (proto.Message, error) {
	config := &socks.ClientConfig{
		NoPipeline:  v.Pipeline != nil && !*v.Pipeline,
		NoPrewarm:   v.Prewarm != nil && !*v.Prewarm,
		NoEarlyData: v.EarlyData != nil && !*v.EarlyData,
	}
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
					return nil, errors.New("failed to parse Socks user").Base(err)
				}
			}
			account := new(SocksAccount)
			if v.Address != nil {
				account.Username = v.Username
				account.Password = v.Password
			} else {
				if err := json.Unmarshal(rawUser, account); err != nil {
					return nil, errors.New("failed to parse socks account").Base(err)
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

func (v *SocksAccount) runtimeUser() *protocol.User {
	if v.Email == "" {
		return nil
	}
	return &protocol.User{Email: v.Email}
}
