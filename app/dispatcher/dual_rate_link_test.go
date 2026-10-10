package dispatcher

import (
	"context"
	"testing"
	"time"

	statsapp "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// 双速率（PIR/CIR/CBS）**穿过真实 dispatcher** 的端到端测试。
//
// 已有的测试分别守住了两头：`common/protocol/dual_rate_test.go` 证明桶串对了，
// `infra/conf` 的矩阵证明各协议都解析出了这三个字段。中间这一段——
// dispatcher 到底有没有把这串桶挂到 link 上、有没有挂在**正确的方向上**——
// 此前只有编译期保证。这个文件补的就是这一段：真的推字节过去，量速率。
//
// 为什么要按方向分开测：`getLink` 造的是**两条独立管道**（不是 WrapLink 那种
// 单条双向 link），限速器必须在两条管道的读端各套一次。历史上这里错过一次，
// 结果是上行完全不限速——而只测一个方向的话，另一个方向漏了你根本不会知道。
//
// 时间常数是真实时钟量出来的，所以边界留得宽；判据是「两段速率差一个数量级，
// 且稳态落在 CIR 附近」，不是精确值。
const (
	// PIR 4 MB/s（峰值桶 burst = 1/8 秒 = 500KB）
	dualPeakBitsPerSec = 32_000_000
	// CIR 400 KB/s
	dualCommittedBitsPerSec = 3_200_000
	// CBS 2 MB：够第一段跑满峰值，又能在第二段之内花完
	dualBurstBytes = 2_000_000

	dualPeakBytesPerSec      = dualPeakBitsPerSec / 8
	dualCommittedBytesPerSec = dualCommittedBitsPerSec / 8

	// 三段窗口：突发段量 PIR，中间段把 CBS 烧干（不断言），稳态段量 CIR。
	dualBurstWindow  = 200 * time.Millisecond
	dualDrainWindow  = 600 * time.Millisecond
	dualSteadyWindow = 1000 * time.Millisecond

	// 管道内在途字节上限。不设的话不限速的那一端会一路狂写、把内存吃光；
	// 设小了又会让「阻塞在管道上」冒充「阻塞在限速桶上」。64KB 两头都够。
	dualPipeBufferBytes = 64 * 1024
)

// dualRateStep 在 window 时间内尽可能多地搬运字节，返回实际搬运量与**实际**耗时。
// 按实际耗时算速率，所以最后一次读/写超出窗口边界不会污染结果。
type dualRateStep func(window time.Duration) (bytes uint64, elapsed time.Duration)

// drainStep 量的是「读端被限住」的情形（getLink 两条管道、WrapLink 的 Reader）。
func drainStep(t *testing.T, r buf.Reader) dualRateStep {
	t.Helper()
	return func(window time.Duration) (uint64, time.Duration) {
		var total uint64
		start := time.Now()
		for time.Since(start) < window {
			mb, err := r.ReadMultiBuffer()
			total += uint64(mb.Len())
			buf.ReleaseMulti(mb)
			if err != nil {
				t.Fatalf("读取中断：%v（喂数据的 goroutine 死了？）", err)
			}
		}
		return total, time.Since(start)
	}
}

// writeStep 量的是「写端被限住」的情形（WrapLink 的 Writer）。
func writeStep(t *testing.T, w buf.Writer) dualRateStep {
	t.Helper()
	return func(window time.Duration) (uint64, time.Duration) {
		var total uint64
		start := time.Now()
		for time.Since(start) < window {
			b := buf.New()
			b.Extend(buf.Size)
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				t.Fatalf("写入中断：%v", err)
			}
			total += buf.Size
		}
		return total, time.Since(start)
	}
}

// feed 起一个「有多少写多少」的不限速生产者，直到 ctx 结束或管道关闭。
// 它自己不设限速，所以量出来的速率就是被测限速器的速率。
func feed(ctx context.Context, w buf.Writer) {
	go func() {
		for ctx.Err() == nil {
			b := buf.New()
			b.Extend(buf.Size)
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				return
			}
		}
	}()
}

// sink 起一个「有多少读多少」的不限速消费者。
func sink(ctx context.Context, r buf.Reader) {
	go func() {
		for ctx.Err() == nil {
			mb, err := r.ReadMultiBuffer()
			buf.ReleaseMulti(mb)
			if err != nil {
				return
			}
		}
	}()
}

func dualRate(bytes uint64, elapsed time.Duration) float64 {
	return float64(bytes) / elapsed.Seconds()
}

// assertDualRate 是这个文件的全部断言：先跑一段突发（应在 PIR 附近），
// 烧干 CBS，再跑一段稳态（应落在 CIR 附近）。
func assertDualRate(t *testing.T, step dualRateStep) {
	t.Helper()

	burstRate := dualRate(step(dualBurstWindow))
	t.Logf("突发段：%.0f B/s（PIR = %d B/s）", burstRate, dualPeakBytesPerSec)
	if burstRate < dualPeakBytesPerSec/2 {
		t.Errorf("突发段只有 %.0f B/s，不到 PIR(%d) 的一半——客户买了峰值速率却一开始就跑不满",
			burstRate, dualPeakBytesPerSec)
	}
	if burstRate > 3*dualPeakBytesPerSec {
		t.Errorf("突发段 %.0f B/s 远超 PIR(%d)——峰值桶根本没挂上（这个方向不限速）",
			burstRate, dualPeakBytesPerSec)
	}

	step(dualDrainWindow) // 把 CBS 烧干，不断言

	steadyRate := dualRate(step(dualSteadyWindow))
	t.Logf("稳态段：%.0f B/s（CIR = %d B/s）", steadyRate, dualCommittedBytesPerSec)
	if steadyRate < 0.7*dualCommittedBytesPerSec {
		t.Errorf("稳态段只有 %.0f B/s，低于承诺速率 %d——客户拿不到卖给他的 CIR",
			steadyRate, dualCommittedBytesPerSec)
	}
	if steadyRate > 1.4*dualCommittedBytesPerSec {
		t.Errorf("稳态段 %.0f B/s，高于承诺速率 %d——承诺桶没串上，突发额度花完了还在按峰值跑",
			steadyRate, dualCommittedBytesPerSec)
	}
	if burstRate < 4*steadyRate {
		t.Errorf("突发段(%.0f B/s)与稳态段(%.0f B/s)没拉开差距——双速率退化成了单速率",
			burstRate, steadyRate)
	}
}

func dualRateUser(t *testing.T, email string) *protocol.MemoryUser {
	t.Helper()
	u := &protocol.MemoryUser{
		Email:               email,
		BandwidthBps:        dualPeakBitsPerSec,
		CommittedBps:        dualCommittedBitsPerSec,
		CommittedBurstBytes: dualBurstBytes,
	}
	// 桶按 *MemoryUser 指针缓存：每个子测试一个新用户 = 一套满的新桶，互不干扰。
	t.Cleanup(u.ResetRuntimeLimiter)
	return u
}

func dualRateContext(t *testing.T, user *protocol.MemoryUser) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ctx = policy.ContextWithBufferPolicy(ctx, policy.Buffer{PerConnection: dualPipeBufferBytes})
	return session.ContextWithInbound(ctx, &session.Inbound{
		User:   user,
		Source: net.TCPDestination(net.LocalHostIP, 12345),
	})
}

func dualRateStats(t *testing.T) stats.Manager {
	t.Helper()
	m, err := statsapp.NewManager(context.Background(), &statsapp.Config{})
	if err != nil {
		t.Fatalf("建 stats manager 失败：%v", err)
	}
	return m
}

// getLink 造的两条管道：上行、下行必须**各自**被双速率限住。
func TestGetLinkDualRateBothDirections(t *testing.T) {
	d := &DefaultDispatcher{policy: policy.DefaultManager{}, stats: dualRateStats(t)}

	t.Run("uplink", func(t *testing.T) {
		ctx := dualRateContext(t, dualRateUser(t, "up@dualrate.test"))
		in, out := d.getLink(ctx, net.Destination{})
		// 上行管道：写 inboundLink.Writer，读 outboundLink.Reader。
		feed(ctx, in.Writer)
		t.Cleanup(func() { common.Close(in.Writer) })
		assertDualRate(t, drainStep(t, out.Reader))
	})

	t.Run("downlink", func(t *testing.T) {
		ctx := dualRateContext(t, dualRateUser(t, "down@dualrate.test"))
		in, out := d.getLink(ctx, net.Destination{})
		// 下行管道：写 outboundLink.Writer，读 inboundLink.Reader。
		feed(ctx, out.Writer)
		t.Cleanup(func() { common.Close(out.Writer) })
		assertDualRate(t, drainStep(t, in.Reader))
	})
}

// WrapLink 包的是单条双向 link：Reader（上行）与 Writer（下行）都要被限住。
func TestWrapLinkDualRateBothDirections(t *testing.T) {
	statsManager := dualRateStats(t)

	t.Run("reader", func(t *testing.T) {
		ctx := dualRateContext(t, dualRateUser(t, "wrap-read@dualrate.test"))
		pr, pw := pipe.New(pipe.WithSizeLimit(dualPipeBufferBytes))
		link := WrapLink(ctx, policy.DefaultManager{}, statsManager,
			&transport.Link{Reader: pr, Writer: buf.Discard})
		feed(ctx, pw)
		t.Cleanup(func() { common.Close(pw) })
		assertDualRate(t, drainStep(t, link.Reader))
	})

	t.Run("writer", func(t *testing.T) {
		ctx := dualRateContext(t, dualRateUser(t, "wrap-write@dualrate.test"))
		pr, pw := pipe.New(pipe.WithSizeLimit(dualPipeBufferBytes))
		emptyReader, emptyWriter := pipe.New()
		t.Cleanup(func() { common.Close(emptyWriter) })
		link := WrapLink(ctx, policy.DefaultManager{}, statsManager,
			&transport.Link{Reader: emptyReader, Writer: pw})
		// 下游有多少吃多少，所以卡住写端的只可能是限速桶。
		sink(ctx, pr)
		assertDualRate(t, writeStep(t, link.Writer))
	})
}
