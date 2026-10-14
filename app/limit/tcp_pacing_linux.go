//go:build linux

package limit

import (
	"context"
	"fmt"
	"math"
	"syscall"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
	"golang.org/x/sys/unix"
)

type SocketPacingError struct{ Cause error }

func (err *SocketPacingError) Error() string { return fmt.Sprintf("limit.socketPacing: %v", err.Cause) }
func (err *SocketPacingError) Unwrap() error { return err.Cause }

type socketPacer struct {
	connection syscall.RawConn
	lastRate   uint64
}

func newSocketPacer(ctx context.Context) (*socketPacer, error) {
	inbound := session.InboundFromContext(ctx)
	if inbound == nil || inbound.Conn == nil {
		return nil, nil
	}
	connection, _, _ := proxy.UnwrapRawConn(inbound.Conn)
	if connection == nil {
		return nil, nil
	}
	if _, tcp := connection.RemoteAddr().(*net.TCPAddr); !tcp {
		return nil, nil
	}
	owner, ok := connection.(syscall.Conn)
	// 多路复用传输不会暴露独占 TCP fd；不能把一个会话的份额施加给整个传输。
	if !ok {
		return nil, nil
	}
	raw, err := owner.SyscallConn()
	if err != nil {
		return nil, &SocketPacingError{Cause: err}
	}
	return &socketPacer{connection: raw}, nil
}
func (pacer *socketPacer) update(rate float64) error {
	if pacer == nil || rate <= 0 {
		return nil
	}
	var next uint64
	if math.IsInf(rate, 1) || rate >= float64(math.MaxUint64) {
		if pacer.lastRate == 0 {
			return nil
		}
		next = math.MaxUint64
	} else {
		next = uint64(math.Ceil(rate))
	}
	if pacer.lastRate > 0 && math.Abs(float64(next)-float64(pacer.lastRate))/float64(pacer.lastRate) < 0.1 {
		return nil
	}
	var optionError error
	controlError := pacer.connection.Control(func(descriptor uintptr) {
		optionError = unix.SetsockoptUint64(int(descriptor), unix.SOL_SOCKET, unix.SO_MAX_PACING_RATE, next)
	})
	if controlError != nil {
		return &SocketPacingError{Cause: controlError}
	}
	if optionError != nil {
		return &SocketPacingError{Cause: optionError}
	}
	pacer.lastRate = next
	return nil
}
