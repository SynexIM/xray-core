package socks

import (
	"context"
	goerrors "errors"
	"slices"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/retry"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// 预热连接（fork）：按最近的需求，提前和上游建好 TCP 并做完方法协商与认证，停在"等请求"。
// 新连接来了直接发请求：到上游的往返从"TCP 握手 + 握手"降到只剩请求本身。
//
// 默认开启、自动回退：
//   - 只在近 warmWindow 内有过连接时补，数量跟着需求走，最多 warmMax 条；没人用就不补。
//   - 预热连接空闲 warmMaxIdle 就关（实测上游会掐掉认证后 60 秒不发请求的连接）。
//   - 拿到的预热连接失效就当场换新连接重来，客户端无感；连续失效 warmMaxMisses 次，
//     说明这个上游不适合预热，停用 warmBackoff。
//   - 补连接失败按指数退避（200ms 起翻倍，封顶 10 秒），成功即复位：上游断了也不会被反复狂拨
//     （Xray VLESS testpre 吃过这个亏，见 XTLS/Xray-core#7113）。补连接只由真实请求触发，不自转。
//   - 用户名口令认证的上游，预热时问候与认证一次写完，需要流水线可用；被判不支持流水线，预热随之停。
const (
	warmMax       = 4
	warmWindow    = 10 * time.Second
	warmMaxIdle   = 20 * time.Second
	warmMaxMisses = 3
	warmBackoff   = time.Hour
	warmRetryMin  = 200 * time.Millisecond
	warmRetryMax  = 10 * time.Second
)

type warmConn struct {
	conn stat.Connection
	at   time.Time
}

type warmPool struct {
	disabled bool // 配置里显式关掉

	mu          sync.Mutex
	idle        []*warmConn
	filling     int
	windowStart time.Time
	demand      int // 本窗口内要过连接的次数
	lastDemand  int // 上一个窗口的
	misses      int
	backoffTill time.Time
	retryDelay  time.Duration // 补连接失败后的退避；0 = 没有失败
	retryTill   time.Time
}

func (w *warmPool) target(now time.Time) int {
	w.roll(now)
	if w.disabled || now.Before(w.backoffTill) || now.Before(w.retryTill) {
		return 0
	}
	return min(warmMax, max(w.demand, w.lastDemand))
}

func (w *warmPool) roll(now time.Time) {
	if now.Sub(w.windowStart) < warmWindow {
		return
	}
	if now.Sub(w.windowStart) < 2*warmWindow {
		w.lastDemand = w.demand
	} else {
		w.lastDemand = 0
	}
	w.demand, w.windowStart = 0, now
}

// take 记一次需求，取最新的一条还没过期的预热连接；没有返回 nil。
func (w *warmPool) take(now time.Time) stat.Connection {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.roll(now)
	w.demand++
	for len(w.idle) > 0 {
		wc := w.idle[len(w.idle)-1]
		w.idle = w.idle[:len(w.idle)-1]
		if now.Sub(wc.at) < warmMaxIdle {
			return wc.conn
		}
		wc.conn.Close()
	}
	return nil
}

func (w *warmPool) hit() {
	w.mu.Lock()
	w.misses = 0
	w.mu.Unlock()
}

// miss：预热连接拿来用时已失效。
func (w *warmPool) miss(now time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.misses++; w.misses < warmMaxMisses {
		return
	}
	w.misses, w.backoffTill = 0, now.Add(warmBackoff)
	for _, wc := range w.idle {
		wc.conn.Close()
	}
	w.idle = nil
}

// refill 补到目标数量；每条在后台建、认证，完成后放进池里，空闲到期自行关闭。
func (w *warmPool) refill(open func() (stat.Connection, error)) {
	w.mu.Lock()
	need := w.target(time.Now()) - len(w.idle) - w.filling
	if need <= 0 {
		w.mu.Unlock()
		return
	}
	w.filling += need
	w.mu.Unlock()
	for range need {
		go func() {
			conn, err := open()
			w.mu.Lock()
			defer w.mu.Unlock()
			w.filling--
			if err != nil {
				w.retryDelay = min(max(2*w.retryDelay, warmRetryMin), warmRetryMax)
				w.retryTill = time.Now().Add(w.retryDelay)
				return
			}
			w.retryDelay = 0
			wc := &warmConn{conn: conn, at: time.Now()}
			w.idle = append(w.idle, wc)
			time.AfterFunc(warmMaxIdle, func() {
				w.mu.Lock()
				defer w.mu.Unlock()
				if i := slices.Index(w.idle, wc); i >= 0 {
					w.idle = slices.Delete(w.idle, i, i+1)
					wc.conn.Close()
				}
			})
		}()
	}
}

// connect 拿到一条已经对上游发完请求、读完回复的连接：预热连接 → 流水线握手 → 逐段握手。
func (c *Client) connect(ctx context.Context, dialer internet.Dialer, request *protocol.RequestHeader, p policy.Session) (stat.Connection, *protocol.RequestHeader, error) {
	dest := c.server.Destination
	dial := func(ctx context.Context) (stat.Connection, error) {
		var conn stat.Connection
		err := retry.ExponentialBackoff(5, 100).On(func() error {
			raw, err := dialer.Dial(ctx, dest)
			conn = raw
			return err
		})
		if err != nil {
			return nil, errors.New("failed to find an available destination").Base(err)
		}
		return conn, nil
	}
	pipelined := request.User != nil && c.pipeline.usable(time.Now())
	detached := context.WithoutCancel(ctx)
	defer c.warm.refill(func() (stat.Connection, error) {
		if request.User != nil && !c.pipeline.usable(time.Now()) {
			return nil, errors.New("pipelining unavailable")
		}
		conn, err := dial(detached)
		if err != nil {
			return nil, err
		}
		_ = conn.SetDeadline(time.Now().Add(p.Timeouts.Handshake))
		if err := pipelinedAuth(request.User, nil, conn, conn, time.Now().Add(p.Timeouts.Handshake)); err != nil {
			conn.Close()
			return nil, err
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	})

	if conn := c.warm.take(time.Now()); conn != nil {
		_ = conn.SetDeadline(time.Now().Add(p.Timeouts.Handshake))
		udp, err := requestOnWarm(request, conn)
		var refused *handshakeRefused
		switch {
		case err == nil:
			c.warm.hit()
			return conn, udp, nil
		case goerrors.As(err, &refused):
			c.warm.hit() // 连接是好的，是请求本身被拒
			conn.Close()
			return nil, nil, err
		}
		conn.Close()
		c.warm.miss(time.Now())
		errors.LogDebugInner(ctx, err, "warm connection to ", dest, " went stale, dialing a fresh one")
	}

	conn, err := dial(ctx)
	if err != nil {
		return nil, nil, err
	}
	deadline := time.Now().Add(p.Timeouts.Handshake)
	_ = conn.SetDeadline(deadline)
	if !pipelined {
		udp, err := ClientHandshake(request, conn, conn)
		return finish(conn, udp, err)
	}
	udp, err := ClientHandshakePipelined(request, conn, conn, deadline)
	if err == nil {
		c.pipeline.proven.Store(true)
		return conn, udp, nil
	}
	if !c.pipeline.suspect(err) {
		return finish(conn, udp, err)
	}
	// 换一条连接逐段握手；逐段成功才说明是流水线的问题，记住这个上游。
	cause := err
	conn.Close()
	if conn, err = dial(ctx); err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(p.Timeouts.Handshake))
	if udp, err = ClientHandshake(request, conn, conn); err == nil {
		c.pipeline.reject(time.Now())
		errors.LogWarningInner(ctx, cause, "socks upstream ", dest, " needs a sequential handshake; not pipelining for ", pipelineBackoff)
	}
	return finish(conn, udp, err)
}

func finish(conn stat.Connection, udp *protocol.RequestHeader, err error) (stat.Connection, *protocol.RequestHeader, error) {
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, udp, nil
}
