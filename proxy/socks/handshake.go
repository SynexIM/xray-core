package socks

import (
	"io"
	"strconv"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
)

// 错开发送（fork）：问候、认证、请求、首包各自一次写，彼此隔 gap，不等上一段的回复，最后按序读回复。
//
// 为什么不一次写出：真实上游里有一类服务端读问候时把同一次读到的后续字节一并丢掉
// （cpxy-ng#85、cfal/shoes#159；实测某住宅代理商 7 个入口里 6 个如此）。各段错开几毫秒到达，
// 它读完问候后再读认证时，认证字节还在缓冲区里。同一批入口实测：错开 2–50ms 全部成功，
// 首字节时间约减半（774ms → 407ms）；一次写出 0 成功。
//
// 首包随请求单独判断：CONNECT 后隔 earlyGap 发首包，有的入口保得住、有的稳定丢掉（实测 4/7 支持，
// 间隔 2ms 时偶有丢失、5ms 全部成功），所以只在自检确认后启用，间隔取 10ms 留余量。
const (
	stageGap = 5 * time.Millisecond
	earlyGap = 10 * time.Millisecond
)

// stageStalled：在请求回复之前就卡住或断开——这个上游可能不吃错开发送，值得逐段重试一次。
type stageStalled struct{ cause error }

func (e *stageStalled) Error() string {
	return "upstream stalled on a staggered SOCKS5 handshake: " + e.cause.Error()
}

func (e *stageStalled) Unwrap() error { return e.cause }

// handshakeRefused：服务端明确回了"不"（认证失败、请求被拒），换什么握手方式都一样。
type handshakeRefused struct{ msg string }

func (e *handshakeRefused) Error() string { return e.msg }

type deadlineReadWriter interface {
	io.ReadWriter
	SetReadDeadline(time.Time) error
}

func greeting(user *protocol.MemoryUser) []byte {
	if user != nil {
		return []byte{socks5Version, 0x01, authPassword}
	}
	return []byte{socks5Version, 0x01, authNotRequired}
}

func credentials(user *protocol.MemoryUser) []byte {
	account := user.Account.(*Account)
	b := make([]byte, 0, 3+len(account.Username)+len(account.Password))
	b = append(b, 0x01, byte(len(account.Username)))
	b = append(b, account.Username...)
	b = append(b, byte(len(account.Password)))
	return append(b, account.Password...)
}

func requestBytes(request *protocol.RequestHeader) ([]byte, error) {
	b := buf.New()
	defer b.Release()
	if err := writeClientRequest(b, request); err != nil {
		return nil, err
	}
	return append([]byte(nil), b.Bytes()...), nil
}

// authenticate 按 RFC 1928 逐段完成方法协商与认证（预热连接在后台做，不在客户的关键路径上）。
func authenticate(user *protocol.MemoryUser, conn io.ReadWriter) error {
	if err := buf.WriteAllBytes(conn, greeting(user), nil); err != nil {
		return err
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[0] != socks5Version || reply[1] != greeting(user)[2] {
		return errors.New("unexpected method reply ", reply[0], " ", reply[1])
	}
	if user == nil {
		return nil
	}
	if err := buf.WriteAllBytes(conn, credentials(user), nil); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, reply); err != nil {
		return err
	}
	if reply[1] != 0x00 {
		return &handshakeRefused{"server rejects account: " + strconv.Itoa(int(reply[1]))}
	}
	return nil
}

// sendRequest 在已认证的连接上发请求；early 非空时隔 earlyGap 紧跟着发首包，不等回复。
// 返回前读完请求回复。
func sendRequest(request *protocol.RequestHeader, early []byte, conn io.ReadWriter) (*protocol.RequestHeader, error) {
	req, err := requestBytes(request)
	if err != nil {
		return nil, err
	}
	if err := buf.WriteAllBytes(conn, req, nil); err != nil {
		return nil, err
	}
	if len(early) > 0 {
		time.Sleep(earlyGap)
		if err := buf.WriteAllBytes(conn, early, nil); err != nil {
			return nil, err
		}
	}
	return readServerReply(request, conn)
}

// staggeredHandshake：认证与请求都不等上一段的回复。请求在认证回复之前就发出，
// 所以和 authenticate 的错开写放在一起：问候 →gap→ 认证 →gap→ 请求 →earlyGap→ 首包，再按序读三段回复。
func staggeredHandshake(request *protocol.RequestHeader, early []byte, conn deadlineReadWriter, deadline time.Time) (*protocol.RequestHeader, error) {
	user := request.User
	req, err := requestBytes(request)
	if err != nil {
		return nil, err
	}
	stages := [][]byte{greeting(user)}
	if user != nil {
		stages = append(stages, credentials(user))
	}
	stages = append(stages, req)
	sent := time.Now()
	for i, s := range stages {
		if i > 0 {
			time.Sleep(stageGap)
		}
		if err := buf.WriteAllBytes(conn, s, nil); err != nil {
			return nil, err
		}
	}
	if len(early) > 0 {
		time.Sleep(earlyGap)
		if err := buf.WriteAllBytes(conn, early, nil); err != nil {
			return nil, err
		}
	}
	reply := make([]byte, 2)
	if _, err := io.ReadFull(conn, reply); err != nil {
		return nil, &stageStalled{err}
	}
	if reply[0] != socks5Version || reply[1] != greeting(user)[2] {
		return nil, &stageStalled{errors.New("unexpected method reply ", reply[0], " ", reply[1])}
	}
	if user != nil {
		_ = conn.SetReadDeadline(time.Now().Add(max(4*time.Since(sent), time.Second)))
		if _, err := io.ReadFull(conn, reply); err != nil {
			return nil, &stageStalled{err}
		}
		_ = conn.SetReadDeadline(deadline)
		if reply[1] != 0x00 {
			return nil, &handshakeRefused{"server rejects account: " + strconv.Itoa(int(reply[1]))}
		}
	}
	return readServerReply(request, conn)
}
