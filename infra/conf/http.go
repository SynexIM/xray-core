package conf

import (
	"encoding/json"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/http"
	"google.golang.org/protobuf/proto"
)

type HTTPAccount struct {
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

func (v *HTTPAccount) Build() *http.Account {
	return &http.Account{
		Username: v.Username,
		Password: v.Password,
	}
}

type HTTPServerConfig struct {
	Users       []*HTTPAccount `json:"users"`
	Accounts    []*HTTPAccount `json:"accounts"`
	Transparent bool           `json:"allowTransparent"`
	UserLevel   uint32         `json:"userLevel"`
}

func (c *HTTPServerConfig) Build() (proto.Message, error) {
	config := &http.ServerConfig{
		AllowTransparent: c.Transparent,
		UserLevel:        c.UserLevel,
	}

	if c.Accounts != nil {
		c.Users = c.Accounts
	}
	// Per-user accounts carry protocol-agnostic limits (bandwidth + connection
	// caps); the legacy `accounts` map (no limits) is left empty to avoid
	// double-registering usernames in the runtime user store.
	if len(c.Users) > 0 {
		config.UserAccounts = make([]*http.UserAccount, 0, len(c.Users))
		for _, account := range c.Users {
			config.UserAccounts = append(config.UserAccounts, &http.UserAccount{
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

	return config, nil
}

type HTTPRemoteConfig struct {
	Address *Address          `json:"address"`
	Port    uint16            `json:"port"`
	Users   []json.RawMessage `json:"users"`
}

type HTTPClientConfig struct {
	Address  *Address            `json:"address"`
	Port     uint16              `json:"port"`
	Level    uint32              `json:"level"`
	Email    string              `json:"email"`
	Username string              `json:"user"`
	Password string              `json:"pass"`
	Servers  []*HTTPRemoteConfig `json:"servers"`
	Headers  map[string]string   `json:"headers"`
}

func (v *HTTPClientConfig) Build() (proto.Message, error) {
	config := new(http.ClientConfig)
	if v.Address != nil {
		v.Servers = []*HTTPRemoteConfig{
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
		return nil, errors.New(`HTTP settings: "servers" should have one and only one member. Multiple endpoints in "servers" should use multiple HTTP outbounds and routing balancer instead`)
	}
	for _, serverConfig := range v.Servers {
		if len(serverConfig.Users) > 1 {
			return nil, errors.New(`HTTP servers: "users" should have one member at most. Multiple members in "users" should use multiple HTTP outbounds and routing balancer instead`)
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
					return nil, errors.New("failed to parse HTTP user").Base(err).AtError()
				}
			}
			account := new(HTTPAccount)
			if v.Address != nil {
				account.Username = v.Username
				account.Password = v.Password
			} else {
				if err := json.Unmarshal(rawUser, account); err != nil {
					return nil, errors.New("failed to parse HTTP account").Base(err).AtError()
				}
			}
			user.Account = serial.ToTypedMessage(account.Build())
			server.User = user
			break
		}
		config.Server = server
		break
	}
	config.Header = make([]*http.Header, 0, 32)
	for key, value := range v.Headers {
		config.Header = append(config.Header, &http.Header{
			Key:   key,
			Value: value,
		})
	}
	return config, nil
}
