package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	. "github.com/xtls/xray-core/infra/conf"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	shadowsocks2022 "github.com/xtls/xray-core/proxy/shadowsocks_2022"
)

func TestShadowsocksServerConfigParsing(t *testing.T) {
	creator := func() Buildable {
		return new(ShadowsocksServerConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"method": "aes-256-GCM",
				"password": "xray-password"
			}`,
			Parser: loadJSON(creator),
			Output: &shadowsocks.ServerConfig{
				Users: []*protocol.User{{
					Account: serial.ToTypedMessage(&shadowsocks.Account{
						CipherType: shadowsocks.CipherType_AES_256_GCM,
						Password:   "xray-password",
					}),
				}},
				Network: []net.Network{net.Network_TCP},
			},
		},
	})
}

func TestShadowsocks2022ExplicitEmptyClientsRemainMultiUser(t *testing.T) {
	const serverKey = "CwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCwsLCws="

	var emptyMulti ShadowsocksServerConfig
	if err := json.Unmarshal([]byte(`{
		"method":"2022-blake3-aes-256-gcm",
		"password":"`+serverKey+`",
		"clients":[]
	}`), &emptyMulti); err != nil {
		t.Fatal(err)
	}
	built, err := emptyMulti.Build()
	if err != nil {
		t.Fatal(err)
	}
	multi, ok := built.(*shadowsocks2022.MultiUserServerConfig)
	if !ok {
		t.Fatalf("explicit clients:[] built %T, want MultiUserServerConfig", built)
	}
	if len(multi.Users) != 0 || multi.Key != serverKey {
		t.Fatalf("empty multi-user config = %#v", multi)
	}

	var legacySingle ShadowsocksServerConfig
	if err := json.Unmarshal([]byte(`{
		"method":"2022-blake3-aes-256-gcm",
		"password":"`+serverKey+`"
	}`), &legacySingle); err != nil {
		t.Fatal(err)
	}
	built, err = legacySingle.Build()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := built.(*shadowsocks2022.ServerConfig); !ok {
		t.Fatalf("omitted clients built %T, want ServerConfig", built)
	}
}
