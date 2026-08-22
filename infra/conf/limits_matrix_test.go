package conf_test

// 全协议限速矩阵。
//
// 这张表回答一个问题：**在配置里给某个协议的用户写上限速，它到底会不会被读进去。**
//
// 之所以要逐个协议测，是因为漏掉一个的表现极其隐蔽：配置写了、面板显示了、
// 保存也成功了，xray 解析时静默丢弃，客户实际不限速——而账面上是限了的。
// 没有测试的话，这种事只会在有人对着账单算流量时才被发现。
//
// 两条都测：设了限速要真的读进去，不设要真的是 0（不限）。
// 后者同样重要——一个"默认值不小心变成非零"的改动会让所有不限速的客户被限。

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/http"
	hysteria "github.com/xtls/xray-core/proxy/hysteria"
	shadowsocks "github.com/xtls/xray-core/proxy/shadowsocks"
	shadowsocks2022 "github.com/xtls/xray-core/proxy/shadowsocks_2022"
	"github.com/xtls/xray-core/proxy/socks"
	trojan "github.com/xtls/xray-core/proxy/trojan"
	vlessinbound "github.com/xtls/xray-core/proxy/vless/inbound"
	vmessinbound "github.com/xtls/xray-core/proxy/vmess/inbound"
	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/infra/conf"
)

const (
	wantBps   = uint64(12_500_000) // PIR 100 Mbps
	wantConn  = uint32(8)
	wantCIR   = uint64(2_500_000)  // CIR 20 Mbps
	wantCBS   = uint64(50_000_000) // CBS 50 MB
	wantClass = "live"             // class（= SKU），拥塞时按它的 weight 争抢
)

// usersOf 把各协议 Build 出来的配置拆出用户列表。
// 每个协议的入站配置类型不同，但用户都是 []*protocol.User——限速就落在那上面。
func usersOf(t *testing.T, msg proto.Message) []*protocol.User {
	t.Helper()
	switch c := msg.(type) {
	case *vlessinbound.Config:
		return c.Users
	case *vmessinbound.Config:
		return c.User
	case *trojan.ServerConfig:
		return c.Users
	case *shadowsocks.ServerConfig:
		return c.Users
	case *shadowsocks2022.MultiUserServerConfig:
		return c.Users
	case *shadowsocks2022.RelayServerConfig:
		// relay 模式没有 User 消息：每个 destination 自带 PSK，它**就是**一个用户。
		// 这里借 RelayDestination.ToMemoryUser（线上 inbound_relay 用的同一段映射）
		// 把它还原成 protocol.User，好让这张矩阵一视同仁地检查它。
		// 走同一个函数是有意的：relay 那边漏搬一个字段，这张表就会红。
		users := make([]*protocol.User, len(c.Destinations))
		for i, d := range c.Destinations {
			mu := d.ToMemoryUser()
			users[i] = &protocol.User{
				Email:               mu.Email,
				Level:               mu.Level,
				BandwidthBps:        mu.BandwidthBps,
				ConnLimit:           mu.ConnLimit,
				CommittedBps:        mu.CommittedBps,
				CommittedBurstBytes: mu.CommittedBurstBytes,
				Class:               mu.Class,
				// relay 的用户身份就是 PSK，本来不经过 protocol.User。补一个
				// account 只是为了让下面 ToMemoryUser 那条断言跑得起来
				// （它要求 account 非空），限速字段与它无关。
				Account: serial.ToTypedMessage(&shadowsocks2022.Account{Key: d.Key}),
			}
		}
		return users
	case *hysteria.ServerConfig:
		return c.Users
	default:
		t.Fatalf("不认识的配置类型 %T，加协议时要一并在这里登记", msg)
		return nil
	}
}

// 每个协议一份最小可用配置。`%s` 处填限速片段，方便同一份配置跑两遍。
var protocols = []struct {
	name string
	// 带限速的配置
	withLimits string
	// 不带限速的同一份配置
	without string
	build   func(raw string) (proto.Message, error)
}{
	{
		name:       "vless",
		withLimits: `{"clients":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}],"decryption":"none"}`,
		without:    `{"clients":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b","email":"a@b"}],"decryption":"none"}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.VLessInboundConfig)) },
	},
	{
		name:       "vmess",
		withLimits: `{"clients":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"clients":[{"id":"27848739-7e62-4138-9fd3-098a63964b6b","email":"a@b"}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.VMessInboundConfig)) },
	},
	{
		name:       "trojan",
		withLimits: `{"clients":[{"password":"pw","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"clients":[{"password":"pw","email":"a@b"}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.TrojanServerConfig)) },
	},
	{
		name:       "shadowsocks-单用户",
		withLimits: `{"method":"aes-128-gcm","password":"pw","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}`,
		without:    `{"method":"aes-128-gcm","password":"pw","email":"a@b"}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.ShadowsocksServerConfig)) },
	},
	{
		name:       "shadowsocks-多用户",
		withLimits: `{"clients":[{"method":"aes-128-gcm","password":"pw","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"clients":[{"method":"aes-128-gcm","password":"pw","email":"a@b"}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.ShadowsocksServerConfig)) },
	},
	{
		// ss2022 多用户走的是 buildShadowsocks2022 这条独立分支，
		// 跟上面那两个 shadowsocks 用例不是同一段代码，所以要单独测。
		name:       "shadowsocks-2022-多用户",
		withLimits: `{"method":"2022-blake3-aes-128-gcm","password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","clients":[{"password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"method":"2022-blake3-aes-128-gcm","password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","clients":[{"password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","email":"a@b"}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.ShadowsocksServerConfig)) },
	},
	{
		// ss2022 relay 又是另一条分支：用户带 address 时 buildShadowsocks2022
		// 走 RelayServerConfig，用户变成 RelayDestination——那上面此前**一个限速
		// 字段都没有**，走 relay 的客户设什么限速都被静默丢掉。
		name:       "shadowsocks-2022-relay",
		withLimits: `{"method":"2022-blake3-aes-128-gcm","password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","clients":[{"password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","email":"a@b","address":"127.0.0.1","port":1234,"bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"method":"2022-blake3-aes-128-gcm","password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","clients":[{"password":"IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w=","email":"a@b","address":"127.0.0.1","port":1234}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.ShadowsocksServerConfig)) },
	},
	{
		name:       "hysteria2",
		withLimits: `{"version":2,"clients":[{"auth":"pw","email":"a@b","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
		without:    `{"version":2,"clients":[{"auth":"pw","email":"a@b"}]}`,
		build:      func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.HysteriaServerConfig)) },
	},
}

type buildable interface{ Build() (proto.Message, error) }

func buildInto(raw string, target buildable) (proto.Message, error) {
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return nil, err
	}
	return target.Build()
}

func TestEveryProtocolReadsLimits(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name+"/设了就限住", func(t *testing.T) {
			msg, err := p.build(p.withLimits)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			users := usersOf(t, msg)
			if len(users) == 0 {
				t.Fatal("没解析出任何用户，配置写错了")
			}
			u := users[0]
			if u.BandwidthBps != wantBps {
				t.Errorf("bandwidth_bps 被丢弃了：期望 %d，实际 %d\n"+
					"  后果：面板显示限速了，xray 实际不限，账面和现实对不上",
					wantBps, u.BandwidthBps)
			}
			if u.ConnLimit != wantConn {
				t.Errorf("conn_limit 被丢弃了：期望 %d，实际 %d", wantConn, u.ConnLimit)
			}
			if u.CommittedBps != wantCIR {
				t.Errorf("committed_bps 被丢弃了：期望 %d，实际 %d\n"+
					"  后果：卖的是承诺速率，节点上只按峰值跑，成本兜不住",
					wantCIR, u.CommittedBps)
			}
			if u.CommittedBurstBytes != wantCBS {
				t.Errorf("committed_burst_bytes 被丢弃了：期望 %d，实际 %d\n"+
					"  后果：突发额度悄悄变成默认的一天承诺量，与合同不符",
					wantCBS, u.CommittedBurstBytes)
			}
			if u.Class != wantClass {
				t.Errorf("class 被丢弃了：期望 %q，实际 %q\n"+
					"  后果：这个客户在节点上落回同权重兜底，卖的直播优先级不生效",
					wantClass, u.Class)
			}
		})

		t.Run(p.name+"/不设就是不限", func(t *testing.T) {
			msg, err := p.build(p.without)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			users := usersOf(t, msg)
			if len(users) == 0 {
				t.Fatal("没解析出任何用户")
			}
			u := users[0]
			if u.BandwidthBps != 0 {
				t.Errorf("没写限速却冒出一个值 %d——所有不限速的客户会被误限", u.BandwidthBps)
			}
			if u.ConnLimit != 0 {
				t.Errorf("没写连接上限却冒出一个值 %d", u.ConnLimit)
			}
			if u.CommittedBps != 0 {
				t.Errorf("没写承诺速率却冒出一个值 %d——所有单速率客户会被多套一个桶", u.CommittedBps)
			}
			if u.CommittedBurstBytes != 0 {
				t.Errorf("没写突发额度却冒出一个值 %d", u.CommittedBurstBytes)
			}
			if u.Class != "" {
				t.Errorf("没写 class 却冒出一个 %q——未分类用户会被塞进某个 SKU 的权重里", u.Class)
			}
		})
	}
}

// 限速一路走到 MemoryUser 才算数：dispatcher 挂限速器读的是 MemoryUser，
// 配置里解析出来但转换时掉了，等于没做。
func TestLimitsSurviveToMemoryUser(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			msg, err := p.build(p.withLimits)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			users := usersOf(t, msg)
			if len(users) == 0 {
				t.Fatal("没解析出任何用户")
			}
			mu, err := users[0].ToMemoryUser()
			if err != nil {
				t.Fatalf("ToMemoryUser 失败：%v", err)
			}
			bps, conns := mu.RuntimeLimits()
			if bps != wantBps || conns != wantConn {
				t.Errorf("限速没能走到 MemoryUser：bandwidth=%d conn=%d", bps, conns)
			}
			if mu.CommittedBps != wantCIR || mu.CommittedBurstBytes != wantCBS {
				t.Errorf("双速率没能走到 MemoryUser：committed=%d burst=%d",
					mu.CommittedBps, mu.CommittedBurstBytes)
			}
			limiters, _ := mu.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
			if len(limiters) == 0 {
				t.Error("限速器没建起来——dispatcher 那边就挂不上东西")
			}
			// CIR < PIR，所以必须是「峰值桶 + 承诺桶」两个；只有一个说明
			// 双速率在这个协议上退化成了单速率。
			if len(limiters) != 2 {
				t.Errorf("双速率没建成两个桶，实际 %d 个", len(limiters))
			} else if got := uint64(limiters[1].Burst()); got != wantCBS {
				t.Errorf("承诺桶的 burst = %d，期望 CBS %d", got, wantCBS)
			}
		})
	}
}

// 不设限速时不该建限流器：建了等于给一个本该无限的用户套上桶。
func TestNoLimitsMeansNoLimiter(t *testing.T) {
	for _, p := range protocols {
		t.Run(p.name, func(t *testing.T) {
			msg, err := p.build(p.without)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			users := usersOf(t, msg)
			mu, err := users[0].ToMemoryUser()
			if err != nil {
				t.Fatalf("ToMemoryUser 失败：%v", err)
			}
			if limiters, _ := mu.RuntimeRateLimiters(buf.NewRateLimiterWithBurst); len(limiters) > 0 {
				t.Error("没设限速却建了限流器")
			}
		})
	}
}

// socks 与 http 是**静态入站**：用户在配置里定义，不走运行时 AddUser，
// 也没有 protocol.User 那条路径。它们把用户放进 UserAccount，
// 启动时汇进一个共享的 UserStore（mixed 入站的 socks 与 http 两面看到的是同一批用户）。
//
// 所以它们的限速要单独测——上面那张矩阵检查不到这条路径。
// accLimits 是静态入站账号上那四个限速字段的快照。
type accLimits struct {
	Bps   uint64
	Conn  uint32
	CIR   uint64
	CBS   uint64
	Class string
}

func TestStaticInboundsReadLimits(t *testing.T) {
	cases := []struct {
		name    string
		with    string
		without string
		build   func(raw string) (proto.Message, error)
		accs    func(proto.Message) []accLimits
	}{
		{
			name:    "socks",
			with:    `{"auth":"password","accounts":[{"user":"u","pass":"p","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
			without: `{"auth":"password","accounts":[{"user":"u","pass":"p"}]}`,
			build:   func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.SocksServerConfig)) },
			accs: func(m proto.Message) []accLimits {
				out := []accLimits{}
				for _, a := range m.(*socks.ServerConfig).UserAccounts {
					out = append(out, accLimits{a.BandwidthBps, a.ConnLimit, a.CommittedBps, a.CommittedBurstBytes, a.Class})
				}
				return out
			},
		},
		{
			name:    "http",
			with:    `{"accounts":[{"user":"u","pass":"p","bandwidth_bps":12500000,"conn_limit":8,"committed_bps":2500000,"committed_burst_bytes":50000000,"class":"live"}]}`,
			without: `{"accounts":[{"user":"u","pass":"p"}]}`,
			build:   func(raw string) (proto.Message, error) { return buildInto(raw, new(conf.HTTPServerConfig)) },
			accs: func(m proto.Message) []accLimits {
				out := []accLimits{}
				for _, a := range m.(*http.ServerConfig).UserAccounts {
					out = append(out, accLimits{a.BandwidthBps, a.ConnLimit, a.CommittedBps, a.CommittedBurstBytes, a.Class})
				}
				return out
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name+"/设了就限住", func(t *testing.T) {
			msg, err := c.build(c.with)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			accs := c.accs(msg)
			if len(accs) == 0 {
				t.Fatal("没解析出任何账号")
			}
			if accs[0].Bps != wantBps || accs[0].Conn != wantConn {
				t.Errorf("限速被丢弃了：bandwidth=%d conn=%d", accs[0].Bps, accs[0].Conn)
			}
			if accs[0].CIR != wantCIR || accs[0].CBS != wantCBS {
				t.Errorf("双速率被丢弃了：committed=%d burst=%d", accs[0].CIR, accs[0].CBS)
			}
			if accs[0].Class != wantClass {
				t.Errorf("class 被丢弃了：期望 %q，实际 %q", wantClass, accs[0].Class)
			}
		})

		t.Run(c.name+"/不设就是不限", func(t *testing.T) {
			msg, err := c.build(c.without)
			if err != nil {
				t.Fatalf("Build 失败：%v", err)
			}
			accs := c.accs(msg)
			if len(accs) == 0 {
				t.Fatal("没解析出任何账号")
			}
			if accs[0].Bps != 0 || accs[0].Conn != 0 {
				t.Errorf("没写限速却冒出值：bandwidth=%d conn=%d", accs[0].Bps, accs[0].Conn)
			}
			if accs[0].CIR != 0 || accs[0].CBS != 0 {
				t.Errorf("没写双速率却冒出值：committed=%d burst=%d", accs[0].CIR, accs[0].CBS)
			}
			if accs[0].Class != "" {
				t.Errorf("没写 class 却冒出一个 %q", accs[0].Class)
			}
		})
	}
}
