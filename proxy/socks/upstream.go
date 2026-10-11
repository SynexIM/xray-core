package socks

import (
	"bytes"
	goerrors "errors"
	"io"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/transport/internet/stat"
)

// 每个出站对自己上游的能力记录（fork）。主人 10-11 定：性能优化默认开、上游不支持自动退回，
// 而且不能让客户撞上"先失败再回退"。所以能力在后台自检得出，测过之前真实流量走逐段握手：
//
//   - 第一次被用到时（之后每 capRecheck 一次）后台经这个上游对 canaryHost 做探测，每项连续
//     capTries 次成功才开启；出现"卡住"（stageStalled、首包发出后没有响应）就判为不支持；
//     连不上、目标不通这类说明不了问题的结果不改变记录，下次再测。
//   - 运行中真实连接若在错开发送时卡住（上游换了软件），立刻降级、逐段重连这一次，到期再测。
//
// 预热连接不需要自检：它在后台按 RFC 1928 逐段认证，任何上游都能用。
const (
	capRecheck    = time.Hour
	capTries      = 3
	canaryHost    = "www.gstatic.com"
	canaryPath    = "/generate_204"
	canaryTimeout = 8 * time.Second
	earlyWait     = 100 * time.Millisecond
)

type capState int8

const (
	capUnknown capState = iota
	capOn
	capOff
)

type upstreamCaps struct {
	noStagger, noEarly bool // 配置里显式关掉

	mu        sync.Mutex
	staggered capState
	early     capState
	checkedAt time.Time
	checking  bool
}

// get 返回当前可用的两项能力。
func (u *upstreamCaps) get() (staggered, early bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return !u.noStagger && u.staggered == capOn, !u.noEarly && u.early == capOn
}

// demote：真实连接在错开发送时卡住，两项都先关掉，到期再测。
func (u *upstreamCaps) demote() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.staggered, u.early, u.checkedAt = capOff, capOff, time.Now()
}

// maybeCheck 到期就在后台自检一次；同一时刻只有一个。
func (u *upstreamCaps) maybeCheck(check func() (staggered, early capState)) {
	if u.noStagger && u.noEarly {
		return
	}
	u.mu.Lock()
	unknown := u.staggered == capUnknown || u.early == capUnknown
	due := !u.checking && (unknown && time.Since(u.checkedAt) >= time.Minute || time.Since(u.checkedAt) >= capRecheck)
	if due {
		u.checking = true
	}
	u.mu.Unlock()
	if !due {
		return
	}
	go func() {
		staggered, early := check()
		u.mu.Lock()
		defer u.mu.Unlock()
		u.checking, u.checkedAt = false, time.Now()
		if staggered != capUnknown {
			u.staggered = staggered
		}
		if early != capUnknown {
			u.early = early
		}
	}()
}

// selfCheck 经 open 拿新连接，分别测错开发送与首包随请求。
func selfCheck(open func() (stat.Connection, error), user *protocol.MemoryUser, noStagger, noEarly bool) (staggered, early capState) {
	request := &protocol.RequestHeader{
		Version: socks5Version, Command: protocol.RequestCommandTCP, User: user,
		Address: net.DomainAddress(canaryHost), Port: 80,
	}
	payload := []byte("GET " + canaryPath + " HTTP/1.1\r\nHost: " + canaryHost + "\r\nConnection: close\r\n\r\n")
	try := func(fn func(conn stat.Connection) error) capState {
		ok := 0
		for range capTries {
			conn, err := open()
			if err != nil {
				return capUnknown
			}
			_ = conn.SetDeadline(time.Now().Add(canaryTimeout))
			err = fn(conn)
			conn.Close()
			var stalled *stageStalled
			switch {
			case err == nil:
				ok++
			case goerrors.As(err, &stalled), goerrors.Is(err, errNoResponse):
				return capOff
			default: // 连不上、被拒、目标不通：说明不了这项能力
				return capUnknown
			}
		}
		if ok == capTries {
			return capOn
		}
		return capUnknown
	}
	if !noStagger {
		staggered = try(func(conn stat.Connection) error {
			if _, err := staggeredHandshake(request, nil, conn, time.Now().Add(canaryTimeout)); err != nil {
				return err
			}
			if _, err := conn.Write(payload); err != nil {
				return err
			}
			if expectHTTP(conn) != nil {
				return errInconclusive // 首包是等回复后才发的：没响应怪不到错开发送
			}
			return nil
		})
	}
	if !noEarly {
		early = try(func(conn stat.Connection) error {
			if err := authenticate(user, conn); err != nil {
				return err
			}
			if _, err := sendRequest(request, payload, conn); err != nil {
				return err
			}
			return expectHTTP(conn)
		})
	}
	return staggered, early
}

var (
	errNoResponse   = goerrors.New("no response after the request was answered")
	errInconclusive = goerrors.New("inconclusive")
)

// takeFirstPayload 在 earlyWait 内取客户端的第一包数据（Xray VLESS 出站同样这么取，它等 500ms）；
// 取不到返回 nil，照原样走。VLESS / VMess / SS / Trojan / Hy2 入站的首包随请求头到达，几乎不等。
func takeFirstPayload(reader buf.Reader) ([]byte, error) {
	tr, ok := reader.(buf.TimeoutReader)
	if !ok {
		return nil, nil
	}
	mb, err := tr.ReadMultiBufferTimeout(earlyWait)
	if err == buf.ErrReadTimeout {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer buf.ReleaseMulti(mb)
	out := make([]byte, mb.Len())
	mb.Copy(out)
	return out, nil
}

// expectHTTP：请求已被上游答应，接下来应当收到 HTTP 响应；收不到说明数据在上游丢了。
func expectHTTP(r io.Reader) error {
	head := make([]byte, 5)
	if _, err := io.ReadFull(r, head); err != nil || !bytes.Equal(head, []byte("HTTP/")) {
		return errNoResponse
	}
	return nil
}
