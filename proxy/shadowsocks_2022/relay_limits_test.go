package shadowsocks_2022

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// ss2022 relay 模式的每用户限速。
//
// relay 模式没有 User 消息——每个 destination 自带 PSK，它本身就是一个用户。
// 所以限速字段此前**完全不存在**：走 relay 的客户在面板里设任何限速都被静默丢掉，
// 而面板不会告诉他。这个文件守住修好之后的两件事：
//
//  1. 每个 destination 对应**一个长期存活**的 MemoryUser（不是每条连接现造一个）。
//     限速桶按 *MemoryUser 指针缓存，现造就等于每条连接一套满桶——开 N 条连接
//     就是 N 倍速率，限速形同虚设。
//  2. 这个 MemoryUser 走 dispatcher 的包装路径时是真的被限住的。

const relayTestPSK = "IdG0eY+zbGDpTEBGKcCSXpuMXNiPUFcbZTHDWbBGb5w="

func newRelayInboundForTest(t *testing.T, dests ...*RelayDestination) *RelayInbound {
	t.Helper()
	if _, err := base64.StdEncoding.DecodeString(relayTestPSK); err != nil {
		t.Fatalf("测试 PSK 写错了：%v", err)
	}
	inbound, err := NewRelayServer(context.Background(), &RelayServerConfig{
		Method:       "2022-blake3-aes-128-gcm",
		Key:          relayTestPSK,
		Destinations: dests,
	})
	if err != nil {
		t.Fatalf("NewRelayServer 失败：%v", err)
	}
	return inbound
}

// relayDest 造一个 destination。四个限速字段全传 0 = 完全不限，
// 这样「不设就是不限」那一条也能用同一个构造器测。
func relayDest(email string, bps, cir, cbs uint64, conns uint32) *RelayDestination {
	return &RelayDestination{
		Key:                 relayTestPSK,
		Email:               email,
		Address:             net.NewIPOrDomain(net.LocalHostIP),
		Port:                1234,
		Level:               2,
		BandwidthBps:        bps,
		CommittedBps:        cir,
		CommittedBurstBytes: cbs,
		ConnLimit:           conns,
	}
}

// destination 上的限速字段必须一路搬进 MemoryUser，而且每次取到的是**同一个**
// 指针——限速桶就挂在这个指针上，换指针 = 换一套新的满桶。
func TestRelayDestinationBecomesOneStableLimitedUser(t *testing.T) {
	const (
		bps = uint64(80_000_000)
		cir = uint64(8_000_000)
		cbs = uint64(50_000_000)
	)
	inbound := newRelayInboundForTest(t,
		relayDest("a@relay.test", bps, cir, cbs, 4),
		relayDest("b@relay.test", 0, 0, 0, 0),
	)

	a := inbound.userAt(0)
	if a == nil {
		t.Fatal("第 0 个 destination 没有对应的用户")
	}
	t.Cleanup(a.ResetRuntimeLimiter)

	if a.Email != "a@relay.test" {
		t.Errorf("email 没搬过来：%q", a.Email)
	}
	if a.Level != 2 {
		t.Errorf("level 没搬过来：%d（upstream 一直漏传，补上后 policy 才按等级生效）", a.Level)
	}
	if a.BandwidthBps != bps || a.CommittedBps != cir || a.CommittedBurstBytes != cbs || a.ConnLimit != 4 {
		t.Errorf("限速字段没搬全：bps=%d cir=%d cbs=%d conn=%d",
			a.BandwidthBps, a.CommittedBps, a.CommittedBurstBytes, a.ConnLimit)
	}
	if !a.HasRuntimeLimits() {
		t.Error("HasRuntimeLimits() = false —— 会被判成不受限而走 splice，一个字节都限不住")
	}

	// 同一个 destination 每次必须给回同一个指针。
	if inbound.userAt(0) != a {
		t.Error("每次取到的是不同的 MemoryUser —— 桶按指针缓存，等于每条连接一套满桶，开 N 条连接就是 N 倍速率")
	}

	limiters, _ := a.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) != 2 {
		t.Errorf("CIR < PIR 应建成「峰值桶 + 承诺桶」两个，实际 %d 个", len(limiters))
	}

	// 没配限速的 destination 不该凭空多出桶来。
	b := inbound.userAt(1)
	if b == nil {
		t.Fatal("第 1 个 destination 没有对应的用户")
	}
	if b.HasRuntimeLimits() {
		t.Error("没配限速的 destination 冒出了限制")
	}
	if l, _ := b.RuntimeRateLimiters(buf.NewRateLimiterWithBurst); len(l) != 0 {
		t.Errorf("没配限速却建了 %d 个桶", len(l))
	}

	// 越界不能 panic —— 拖崩的是整个 xray 进程。
	if inbound.userAt(-1) != nil || inbound.userAt(99) != nil {
		t.Error("越界索引应返回 nil")
	}
}

// 端到端：relay 建出来的用户走 dispatcher 的包装路径时，字节真的被限住。
// 限速的判定与执行都在 dispatcher，这里证明 relay 提供的用户能被它认下来。
func TestRelayUserIsActuallyThrottled(t *testing.T) {
	const (
		bytesPerSec   = 1_000_000
		transferBytes = 1_500_000
		// burst = 1/8 秒 = 125KB，剩下 1.375MB 按 1MB/s 走 ≈1.37s。
		// 不限速的话同样的量几毫秒就搬完了，中间差三个数量级。
		minThrottle = 800 * time.Millisecond
	)
	inbound := newRelayInboundForTest(t, relayDest("throttled@relay.test", bytesPerSec*8, 0, 0, 0))
	user := inbound.userAt(0)
	t.Cleanup(user.ResetRuntimeLimiter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = session.ContextWithInbound(ctx, &session.Inbound{
		User:   user,
		Source: net.TCPDestination(net.LocalHostIP, 12345),
	})

	pr, pw := pipe.New(pipe.WithSizeLimit(64 * 1024))
	link := dispatcher.WrapLink(ctx, policy.DefaultManager{}, nil,
		&transport.Link{Reader: pr, Writer: buf.Discard})
	defer common.Close(pw)

	go func() {
		for ctx.Err() == nil {
			b := buf.New()
			b.Extend(buf.Size)
			if err := pw.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	read := 0
	for read < transferBytes {
		mb, err := link.Reader.ReadMultiBuffer()
		read += int(mb.Len())
		buf.ReleaseMulti(mb)
		if err != nil {
			t.Fatalf("读取中断：%v", err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("搬 %d 字节用了 %v（限速 %d B/s）", transferBytes, elapsed, bytesPerSec)
	if elapsed < minThrottle {
		t.Fatalf("relay 用户 %v 就搬完了 %d 字节（< %v）——限速根本没生效",
			elapsed, transferBytes, minThrottle)
	}
}

// 用户没有 email 时也要能限速（fork 的一贯立场：限速只看用户本身，不看 email）。
// relay 会给空 email 自动补一个 unnamed-destination-*，顺带守住这条。
func TestRelayAutoNamesDestinations(t *testing.T) {
	inbound := newRelayInboundForTest(t, relayDest("", 80_000_000, 0, 0, 0))
	u := inbound.userAt(0)
	t.Cleanup(u.ResetRuntimeLimiter)
	if u.Email == "" {
		t.Fatal("空 email 的 destination 没有被自动命名")
	}
	if !u.HasRuntimeLimits() {
		t.Error("自动命名的 destination 丢了限速")
	}
}
