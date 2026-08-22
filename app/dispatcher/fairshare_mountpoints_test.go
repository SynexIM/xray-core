package dispatcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// 节点级公平限速一共只有两个挂载函数，三个调用点（FR-079a）：
//
//	getLink    Dispatch 的 TCP/UDP 主路径（两条独立管道，读端各挂一次）
//	WrapLink   DispatchLink:492（含 UDP/XUDP）
//	WrapLink   proxy/vless/inbound/inbound.go:639（mux 路径）
//
// 这个文件守住这两个函数**真的会整形**，而不是只把 wrapper 挂上去看着像。
// 断言方式是驱动真实字节并量时间：整形不生效的话 600KB 几毫秒就走完了。
//
// 另外守住一件同样重要的事：同一个用户开 N 条连接，额度不放大。
// limiter 按 *MemoryUser 的 email 在 NodeFairScheduler 里共享，开 10 条连接
// 不该等于 10 倍带宽——那样限速就是摆设。

const (
	fairTestRootCap = uint64(1_000_000) // 1 MB/s
	fairTestBytes   = 600_000
	// burst = root_cap/8 = 125,000 字节先放行，剩下 475,000 按 1MB/s 走 ≈ 475ms。
	// 阈值取 250ms：远高于「没整形」的几毫秒，也留足慢机器的余量。
	fairTestMinThrottle = 250 * time.Millisecond
)

// withNodeFairness 打开进程级节点公平并在测试结束后关掉。
// 顺带清掉 class 表与地板，免得别的用例留下的状态影响这里。
func withNodeFairness(t *testing.T) {
	t.Helper()
	sched := protocol.FairScheduler()
	old := sched.RootCapBytePerSec()
	sched.SetClassPolicies(nil)
	sched.SetFloors(0, 0)
	sched.SetNodeBandwidth(fairTestRootCap)
	t.Cleanup(func() { sched.SetNodeBandwidth(old) })
}

func fairCtx(user *protocol.MemoryUser) context.Context {
	return session.ContextWithInbound(context.Background(), &session.Inbound{
		User:   user,
		Source: net.TCPDestination(net.LocalHostIP, 12345),
	})
}

// getLink 的两条管道读端都要被节点公平整形，TCP 与 UDP 走的是同一条路径。
func TestGetLinkShapesBothPipesUnderNodeFairness(t *testing.T) {
	withNodeFairness(t)
	d := &DefaultDispatcher{policy: policy.DefaultManager{}}

	for _, c := range []struct {
		name string
		dest net.Destination
	}{
		{"TCP", net.TCPDestination(net.LocalHostIP, 80)},
		{"UDP", net.UDPDestination(net.LocalHostIP, 53)},
	} {
		t.Run(c.name+"/uplink", func(t *testing.T) {
			// 每个子用例换一个 email，拿一只满的新桶，互不干扰。
			user := &protocol.MemoryUser{Email: "getlink-up-" + c.name + "@fair.test"}
			in, out := d.getLink(fairCtx(user), c.dest)
			// uplink 管道：写 inboundLink.Writer，读 outboundLink.Reader。
			if elapsed := pumpAndTime(t, in.Writer, out.Reader, fairTestBytes); elapsed < fairTestMinThrottle {
				t.Fatalf("%s 上行 %d 字节只花了 %v（< %v）——节点公平没挂上", c.name, fairTestBytes, elapsed, fairTestMinThrottle)
			}
		})
		t.Run(c.name+"/downlink", func(t *testing.T) {
			user := &protocol.MemoryUser{Email: "getlink-down-" + c.name + "@fair.test"}
			in, out := d.getLink(fairCtx(user), c.dest)
			// downlink 管道：写 outboundLink.Writer，读 inboundLink.Reader。
			if elapsed := pumpAndTime(t, out.Writer, in.Reader, fairTestBytes); elapsed < fairTestMinThrottle {
				t.Fatalf("%s 下行 %d 字节只花了 %v（< %v）——节点公平没挂上", c.name, fairTestBytes, elapsed, fairTestMinThrottle)
			}
		})
	}
}

// newWrappedLink 造一条经过 WrapLink 的 link，并返回喂它/收它的两端。
// WrapLink 就是 DispatchLink:492 与 VLESS mux inbound.go:639 调的那个函数。
func newWrappedLink(t *testing.T, email string) (feed *transport.Link, wrapped *transport.Link, drain *transport.Link) {
	t.Helper()
	upR, upW := pipe.New(pipe.WithSizeLimit(1 << 20))
	downR, downW := pipe.New(pipe.WithSizeLimit(1 << 20))
	user := &protocol.MemoryUser{Email: email}
	link := &transport.Link{Reader: upR, Writer: downW}
	wrapped = WrapLink(fairCtx(user), policy.DefaultManager{}, nil, link)
	return &transport.Link{Writer: upW}, wrapped, &transport.Link{Reader: downR}
}

// WrapLink 的读端与写端都要被整形。
// 下行只在 Writer 上整形（per-user 限速只管上行，节点公平按双向合计算总出口）。
func TestWrapLinkShapesBothDirections(t *testing.T) {
	withNodeFairness(t)

	t.Run("读端（上行）", func(t *testing.T) {
		feed, wrapped, _ := newWrappedLink(t, "wraplink-up@fair.test")
		if elapsed := pumpAndTime(t, feed.Writer, wrapped.Reader, fairTestBytes); elapsed < fairTestMinThrottle {
			t.Fatalf("上行 %d 字节只花了 %v（< %v）——WrapLink 的读端没整形", fairTestBytes, elapsed, fairTestMinThrottle)
		}
	})

	t.Run("写端（下行）", func(t *testing.T) {
		_, wrapped, drain := newWrappedLink(t, "wraplink-down@fair.test")
		if elapsed := pumpAndTime(t, wrapped.Writer, drain.Reader, fairTestBytes); elapsed < fairTestMinThrottle {
			t.Fatalf("下行 %d 字节只花了 %v（< %v）——WrapLink 的写端没整形", fairTestBytes, elapsed, fairTestMinThrottle)
		}
	})
}

// pumpBytes 写入并排空 total 字节，只回错误——供并发子 goroutine 使用。
func pumpBytes(w buf.Writer, r buf.Reader, total int) error {
	writeErr := make(chan error, 1)
	go func() {
		remaining := total
		for remaining > 0 {
			n := remaining
			if n > buf.Size {
				n = buf.Size
			}
			b := buf.New()
			b.Extend(int32(n))
			if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
				writeErr <- err
				return
			}
			remaining -= n
		}
		_ = common.Close(w)
		writeErr <- nil
	}()

	read := 0
	for read < total {
		mb, err := r.ReadMultiBuffer()
		read += int(mb.Len())
		buf.ReleaseMulti(mb)
		if err != nil {
			break
		}
	}
	if err := <-writeErr; err != nil {
		return err
	}
	if read < total {
		return errors.New("only drained ", read, " of ", total, " bytes")
	}
	return nil
}

// 同一个用户开 N 条连接，额度不放大：N 条一起总共只能跑一份带宽。
//
// 这条是限速能不能立住的根本。桶按 email 在 NodeFairScheduler 里共享，
// 一旦变成每条连接一只新桶，客户只要多开几条连接就能把节点吃干。
func TestManyConnectionsShareOneBudget(t *testing.T) {
	withNodeFairness(t)
	d := &DefaultDispatcher{policy: policy.DefaultManager{}}

	const conns = 3
	const perConn = fairTestBytes / conns

	user := &protocol.MemoryUser{Email: "multi@fair.test"}
	links := make([][2]*transport.Link, 0, conns)
	for i := 0; i < conns; i++ {
		in, out := d.getLink(fairCtx(user), net.TCPDestination(net.LocalHostIP, 80))
		links = append(links, [2]*transport.Link{in, out})
	}

	// 不在子 goroutine 里调 t.Fatalf（那不是运行本测试的 goroutine），
	// 出错走 channel 回主 goroutine 报。
	var wg sync.WaitGroup
	errs := make(chan error, conns)
	start := time.Now()
	for _, l := range links {
		wg.Add(1)
		go func(in, out *transport.Link) {
			defer wg.Done()
			errs <- pumpBytes(in.Writer, out.Reader, perConn)
		}(l[0], l[1])
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发灌流失败：%v", err)
		}
	}

	if elapsed < fairTestMinThrottle {
		t.Fatalf("%d 条连接各跑 %d 字节（合计 %d）只花了 %v（< %v）——每条连接一只桶，开连接就能放大额度",
			conns, perConn, fairTestBytes, elapsed, fairTestMinThrottle)
	}
	t.Logf("%d 条连接合计 %d 字节耗时 %v（单条独享一只桶的话约 %v）",
		conns, fairTestBytes, elapsed, time.Duration(perConn)*time.Second/time.Duration(fairTestRootCap))
}
