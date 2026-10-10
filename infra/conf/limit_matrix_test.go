package conf_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/limit"
	"github.com/xtls/xray-core/common/protocol"
	featurelimit "github.com/xtls/xray-core/features/limit"
	"github.com/xtls/xray-core/infra/conf"
	httpProxy "github.com/xtls/xray-core/proxy/http"
	"github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"github.com/xtls/xray-core/proxy/socks"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// 限速规则在协议账户之外；八种协议的启动身份都必须保留同一个 email。
func TestEightProtocolIdentitiesReachOneLimitRule(t *testing.T) {
	cases := []struct{ name, protocol, settings string }{
		{"vless", "vless", `{"decryption":"none","clients":[{"id":"00000000-0000-4000-8000-000000000001","email":"matrix-a"}]}`},
		{"vmess", "vmess", `{"clients":[{"id":"00000000-0000-4000-8000-000000000001","email":"matrix-a"}]}`},
		{"trojan", "trojan", `{"clients":[{"password":"test-only","email":"matrix-a"}]}`},
		{"shadowsocks", "shadowsocks", `{"clients":[{"method":"aes-128-gcm","password":"test-only","email":"matrix-a"}]}`},
		{"shadowsocks2022", "shadowsocks", `{"method":"2022-blake3-aes-128-gcm","password":"AAAAAAAAAAAAAAAAAAAAAA==","clients":[{"password":"AQEBAQEBAQEBAQEBAQEBAQ==","email":"matrix-a"}]}`},
		{"socks", "socks", `{"auth":"password","accounts":[{"user":"auth-key","pass":"test-only","email":"matrix-a"}]}`},
		{"http", "http", `{"accounts":[{"user":"auth-key","pass":"test-only","email":"matrix-a"}]}`},
		{"hysteria2", "hysteria", `{"version":2,"clients":[{"auth":"hy2-test-only","email":"matrix-a"}]}`},
	}
	runtime, err := limit.New(&limit.Config{Profiles: []*limit.Profile{{Name: "p", Down: &limit.Ladder{Steps: []*limit.Step{{RateBps: 8_000_000}}}}}, Rules: []*limit.Rule{{RuleTag: "shared", Profile: "p", User: []string{"matrix-a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	var releases []func()
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var c conf.Config
			data := `{"inbounds":[{"tag":"entry","port":10085,"protocol":"` + test.protocol + `","settings":` + test.settings + `}]}`
			if err := json.Unmarshal([]byte(data), &c); err != nil {
				t.Fatal(err)
			}
			built, err := c.Build()
			if err != nil {
				t.Fatal(err)
			}
			accountConfig, err := built.Inbound[0].ProxySettings.GetInstance()
			if err != nil {
				t.Fatal(err)
			}
			var identity *protocol.MemoryUser
			switch cfg := accountConfig.(type) {
			case interface{ GetUsers() []*protocol.User }:
				identity, err = cfg.GetUsers()[0].ToMemoryUser()
			case interface{ GetUser() []*protocol.User }:
				identity, err = cfg.GetUser()[0].ToMemoryUser()
			case *socks.ServerConfig:
				store := httpProxy.NewUserStore(cfg.UserLevel, cfg.Accounts, cfg.UserAccounts)
				var ok bool
				identity, ok = store.Authenticate("auth-key", "test-only")
				if !ok {
					t.Fatal("SOCKS 认证失败")
				}
			case *httpProxy.ServerConfig:
				store := httpProxy.NewUserStore(cfg.UserLevel, cfg.Accounts, cfg.UserAccounts)
				var ok bool
				identity, ok = store.Authenticate("auth-key", "test-only")
				if !ok {
					t.Fatal("HTTP 认证失败")
				}
			default:
				t.Fatalf("未覆盖的协议配置 %T", accountConfig)
			}
			if err != nil {
				t.Fatal(err)
			}
			if identity.Email != "matrix-a" {
				t.Fatalf("协议身份丢失 email: %+v", identity)
			}
			up, down, release, err := runtime.Attach(context.Background(), featurelimit.SessionInfo{Email: identity.Email, InboundTag: "entry", OutboundTag: "out"})
			if err != nil {
				t.Fatal(err)
			}
			if up == nil || down == nil {
				t.Fatal("规则未挂到两个方向")
			}
			releases = append(releases, release)
		})
	}
	status, ok := runtime.GetRuleStatus("shared")
	if !ok || status.Connections != 8 {
		t.Fatalf("八协议没有归入同一个桶: %+v", status)
	}
}

func TestRelayIdentityAndUpstreamUserShape(t *testing.T) {
	descriptor := (&protocol.User{}).ProtoReflect().Descriptor()
	if descriptor.Fields().Len() != 3 {
		t.Fatal("User 仍含 fork 字段")
	}
	for number := 4; number <= 21; number++ {
		if !descriptor.ReservedRanges().Has(protoreflect.FieldNumber(number)) {
			t.Fatalf("字段号 %d 未保留", number)
		}
	}
	var c conf.Config
	if err := json.Unmarshal([]byte(`{"inbounds":[{"port":10085,"protocol":"shadowsocks","settings":{"method":"2022-blake3-aes-128-gcm","password":"AAAAAAAAAAAAAAAAAAAAAA==","clients":[{"password":"AQEBAQEBAQEBAQEBAQEBAQ==","email":"relay-a","address":"127.0.0.1","port":10086}]}}]}`), &c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := built.Inbound[0].ProxySettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	relay, ok := raw.(*shadowsocks_2022.RelayServerConfig)
	if !ok || len(relay.Destinations) != 1 || relay.Destinations[0].Email != "relay-a" {
		t.Fatalf("relay 身份丢失: %v", raw)
	}
}

func TestMissingEmailCannotMatchUsernameAsUser(t *testing.T) {
	store := httpProxy.NewUserStore[*socks.UserAccount](0, map[string]string{"route-id": "test-only"}, nil)
	identity, ok := store.Authenticate("route-id", "test-only")
	if !ok {
		t.Fatal("上游无 email 账户不能认证")
	}
	r, err := limit.New(&limit.Config{Profiles: []*limit.Profile{{Name: "p"}}, Rules: []*limit.Rule{{RuleTag: "r", Profile: "p", User: []string{"route-id"}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, _, release, err := r.Attach(context.Background(), featurelimit.SessionInfo{Email: identity.Email})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	state, _ := r.GetRuleStatus("r")
	if state.Connections != 0 {
		t.Fatal("没有 email 的用户以用户名命中了 user 条件")
	}
}
