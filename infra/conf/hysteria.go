package conf

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/proxy/hysteria"
	"github.com/xtls/xray-core/proxy/hysteria/account"
	"google.golang.org/protobuf/proto"
)

type HysteriaClientConfig struct {
	Version int32    `json:"version"`
	Address *Address `json:"address"`
	Port    uint16   `json:"port"`
}

func (c *HysteriaClientConfig) Build() (proto.Message, error) {
	if c.Version != 2 {
		return nil, errors.New("version != 2")
	}

	config := &hysteria.ClientConfig{}
	config.Server = &protocol.ServerEndpoint{
		Address: c.Address.Build(),
		Port:    uint32(c.Port),
	}

	return config, nil
}

type HysteriaUserConfig struct {
	Auth  string `json:"auth"`
	Level uint32 `json:"level"`
	Email string `json:"email"`
	// 每用户限速。留空 = 不限。单位是 bit/s，与 protocol.User 的顶层字段同名，
	// 所以所有协议的配置写法完全一致。
	BandwidthBps uint64 `json:"bandwidth_bps"`
	ConnLimit    uint32 `json:"conn_limit"`
	// 双速率（可选）。committed_bps 是承诺速率 CIR，committed_burst_bytes 是
	// 突发额度 CBS（字节，留空 = 一天的承诺量）。语义见 protocol.User。
	CommittedBps        uint64 `json:"committed_bps"`
	CommittedBurstBytes uint64 `json:"committed_burst_bytes"`
}

type HysteriaServerConfig struct {
	Version int32                 `json:"version"`
	Users   []*HysteriaUserConfig `json:"users"`
	Clients []*HysteriaUserConfig `json:"clients"`
}

func (c *HysteriaServerConfig) Build() (proto.Message, error) {
	if c.Version != 2 {
		return nil, errors.New("version != 2")
	}

	config := new(hysteria.ServerConfig)

	if c.Clients != nil {
		c.Users = c.Clients
	}
	if len(c.Users) > 0 {
		config.Users = make([]*protocol.User, len(c.Users))
		processUser := func(idx int) error {
			user := c.Users[idx]
			acc := &account.Account{
				Auth: user.Auth,
			}
			config.Users[idx] = &protocol.User{
				Email:               user.Email,
				Level:               user.Level,
				Account:             serial.ToTypedMessage(acc),
				BandwidthBps:        user.BandwidthBps,
				ConnLimit:           user.ConnLimit,
				CommittedBps:        user.CommittedBps,
				CommittedBurstBytes: user.CommittedBurstBytes,
			}
			return nil
		}
		if err := task.ParallelForN(len(c.Users), processUser); err != nil {
			return nil, err
		}
	}

	return config, nil
}
