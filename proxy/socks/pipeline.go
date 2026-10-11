package socks

import (
	goerrors "errors"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
)

// 流水线握手（fork）：方法协商、用户名口令、请求三段一次发出，再按序读三段回复。
// 上游接受流水线输入（绝大多数都接受）时，首字节前到上游的往返从三次降到一次。
//
// 默认开启、自动回退。不支持的服务端有两种已知表现（cpxy-ng #85、shoes #159）：
// 丢掉问候之后的字节（认证回复迟迟不来），或认证后丢掉提前到的请求（请求回复迟迟不来）。
//   - 只对用户名口令认证的上游做：认证回复不含远端工作，卡住能在 1 秒内认出来；
//     无认证的上游没有这一步可借，提前发请求卡住了分不清是不支持还是目标慢，不冒这个险。
//   - 认证回复之前失败 → 判定不支持；请求回复失败、且这个上游从没成功过一次流水线 → 也可能是不支持。
//     两种都立刻换一条连接逐段握手重来，逐段成功才记住"不支持"，pipelineBackoff 内不再尝试。
//   - 服务端明确拒绝（认证失败、请求被拒）是真结果，不回退。

const pipelineBackoff = time.Hour

// pipelineRejected：失败发生在请求回复之前，换逐段握手重试有意义。
type pipelineRejected struct{ cause error }

func (e *pipelineRejected) Error() string {
	return "upstream does not accept a pipelined SOCKS5 handshake: " + e.cause.Error()
}

func (e *pipelineRejected) Unwrap() error { return e.cause }

// handshakeRefused：服务端明确回了"不"（认证失败、请求被拒），换什么握手方式都一样。
type handshakeRefused struct{ msg string }

func (e *handshakeRefused) Error() string { return e.msg }

// pipelineState 是一个出站对它的上游"支不支持流水线"的记忆。
type pipelineState struct {
	disabled    bool         // 配置里显式关掉
	backoffTill atomic.Int64 // UnixNano；之前不再尝试
	proven      atomic.Bool  // 成功过一次：之后请求阶段的失败都是真失败
}

func (s *pipelineState) usable(now time.Time) bool {
	return !s.disabled && now.UnixNano() >= s.backoffTill.Load()
}

func (s *pipelineState) reject(now time.Time) {
	s.backoffTill.Store(now.Add(pipelineBackoff).UnixNano())
}

// suspect：这次流水线握手的失败可能怪流水线，值得逐段重试一次。
func (s *pipelineState) suspect(err error) bool {
	var rejected *pipelineRejected
	var refused *handshakeRefused
	return goerrors.As(err, &rejected) || (!s.proven.Load() && !goerrors.As(err, &refused))
}

type deadlineReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

// ClientHandshakePipelined 做一次流水线握手。deadline 是整个握手的期限；认证回复不含远端工作，
// 只给它"方法回复耗时的 4 倍、至少 1 秒"，免得在丢字节的服务端上干等到握手超时。
func ClientHandshakePipelined(request *protocol.RequestHeader, reader deadlineReader, writer io.Writer, deadline time.Time) (*protocol.RequestHeader, error) {
	if err := pipelinedAuth(request.User, request, reader, writer, deadline); err != nil {
		return nil, err
	}
	return readServerReply(request, reader)
}

// pipelinedAuth 一次写出方法协商、认证，以及（request 非 nil 时）请求，读完方法与认证两段回复。
// request 为 nil 用于预热连接：认证完停在"等请求"的状态。
func pipelinedAuth(user *protocol.MemoryUser, request *protocol.RequestHeader, reader deadlineReader, writer io.Writer, deadline time.Time) error {
	authByte := byte(authNotRequired)
	if user != nil {
		authByte = byte(authPassword)
	}
	b := buf.New()
	defer b.Release()
	common.Must2(b.Write([]byte{socks5Version, 0x01, authByte}))
	if authByte == authPassword {
		account := user.Account.(*Account)
		common.Must(b.WriteByte(0x01))
		common.Must(b.WriteByte(byte(len(account.Username))))
		common.Must2(b.WriteString(account.Username))
		common.Must(b.WriteByte(byte(len(account.Password))))
		common.Must2(b.WriteString(account.Password))
	}
	if request != nil {
		if err := writeClientRequest(b, request); err != nil {
			return err
		}
	}
	sent := time.Now()
	if err := buf.WriteAllBytes(writer, b.Bytes(), nil); err != nil {
		return err
	}

	b.Clear()
	if _, err := b.ReadFullFrom(reader, 2); err != nil {
		return &pipelineRejected{err}
	}
	if b.Byte(0) != socks5Version || b.Byte(1) != authByte {
		return &pipelineRejected{errors.New("unexpected method reply ", b.Byte(0), " ", b.Byte(1))}
	}
	if authByte == authPassword {
		rtt := time.Since(sent)
		_ = reader.SetReadDeadline(time.Now().Add(max(4*rtt, time.Second)))
		b.Clear()
		if _, err := b.ReadFullFrom(reader, 2); err != nil {
			return &pipelineRejected{err}
		}
		_ = reader.SetReadDeadline(deadline)
		if b.Byte(1) != 0x00 {
			// 认证真被拒：逐段握手也一样会被拒，不回退。
			return &handshakeRefused{"server rejects account: " + strconv.Itoa(int(b.Byte(1)))}
		}
	}
	return nil
}

// requestOnWarm 在已认证的预热连接上发请求、读回复。
func requestOnWarm(request *protocol.RequestHeader, conn io.ReadWriter) (*protocol.RequestHeader, error) {
	b := buf.New()
	defer b.Release()
	if err := writeClientRequest(b, request); err != nil {
		return nil, err
	}
	if err := buf.WriteAllBytes(conn, b.Bytes(), nil); err != nil {
		return nil, err
	}
	return readServerReply(request, conn)
}
