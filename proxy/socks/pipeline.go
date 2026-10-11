package socks

import (
	"io"
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
// 默认开启、自动回退：某个上游在请求回复之前就失败（断开、回复不对、认证回复迟迟不来——
// 只读了第一段就丢掉后面字节的服务端会这样），就判定它不支持，这个出站在 pipelineBackoff
// 内改回逐段握手，并立刻用逐段握手重连本次请求。请求本身被拒（目标不可达等）不算不支持。

const pipelineBackoff = time.Hour

// pipelineRejected：失败发生在请求回复之前，换逐段握手重试有意义。
type pipelineRejected struct{ cause error }

func (e *pipelineRejected) Error() string {
	return "upstream does not accept a pipelined SOCKS5 handshake: " + e.cause.Error()
}

func (e *pipelineRejected) Unwrap() error { return e.cause }

// pipelineState 是一个出站对它的上游"支不支持流水线"的记忆。
type pipelineState struct {
	disabled    bool         // 配置里显式关掉
	backoffTill atomic.Int64 // UnixNano；之前不再尝试
}

func (s *pipelineState) usable(now time.Time) bool {
	return !s.disabled && now.UnixNano() >= s.backoffTill.Load()
}

func (s *pipelineState) reject(now time.Time) {
	s.backoffTill.Store(now.Add(pipelineBackoff).UnixNano())
}

type deadlineReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

// ClientHandshakePipelined 做一次流水线握手。deadline 是整个握手的期限；认证回复不含远端工作，
// 只给它"方法回复耗时的 4 倍、至少 1 秒"，免得在丢字节的服务端上干等到握手超时。
func ClientHandshakePipelined(request *protocol.RequestHeader, reader deadlineReader, writer io.Writer, deadline time.Time) (*protocol.RequestHeader, error) {
	authByte := byte(authNotRequired)
	if request.User != nil {
		authByte = byte(authPassword)
	}
	b := buf.New()
	defer b.Release()
	common.Must2(b.Write([]byte{socks5Version, 0x01, authByte}))
	if authByte == authPassword {
		account := request.User.Account.(*Account)
		common.Must(b.WriteByte(0x01))
		common.Must(b.WriteByte(byte(len(account.Username))))
		common.Must2(b.WriteString(account.Username))
		common.Must(b.WriteByte(byte(len(account.Password))))
		common.Must2(b.WriteString(account.Password))
	}
	if err := writeClientRequest(b, request); err != nil {
		return nil, err
	}
	sent := time.Now()
	if err := buf.WriteAllBytes(writer, b.Bytes(), nil); err != nil {
		return nil, err
	}

	b.Clear()
	if _, err := b.ReadFullFrom(reader, 2); err != nil {
		return nil, &pipelineRejected{err}
	}
	if b.Byte(0) != socks5Version || b.Byte(1) != authByte {
		return nil, &pipelineRejected{errors.New("unexpected method reply ", b.Byte(0), " ", b.Byte(1))}
	}
	if authByte == authPassword {
		rtt := time.Since(sent)
		_ = reader.SetReadDeadline(time.Now().Add(max(4*rtt, time.Second)))
		b.Clear()
		if _, err := b.ReadFullFrom(reader, 2); err != nil {
			return nil, &pipelineRejected{err}
		}
		_ = reader.SetReadDeadline(deadline)
		if b.Byte(1) != 0x00 {
			// 认证真被拒：逐段握手也一样会被拒，不回退。
			return nil, errors.New("server rejects account: ", b.Byte(1))
		}
	}
	return readServerReply(request, reader)
}
