package mux

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport/pipe"
)

// 限速包装过的 link 上的 XUDP 会话关闭：不能 panic（旧代码断言 *pipe.Reader，整个内核崩），
// 且非 XUDP 会话的打断要真的传到管道。
type passPacer struct{}

func (passPacer) Wait(context.Context, int) error { return nil }

func TestSessionCloseOnRateLimitedInput(t *testing.T) {
	for _, xudp := range []bool{true, false} {
		reader, writer := pipe.New()
		m := NewSessionManager()
		s := &Session{
			input:  buf.NewPacedReader(context.Background(), reader, passPacer{}),
			output: writer,
			parent: m,
		}
		if xudp {
			s.XUDP = &XUDP{Status: Active}
		}
		m.Add(s)
		if err := s.Close(false); err != nil {
			t.Fatal(err)
		}
		if !xudp {
			if _, err := reader.ReadMultiBuffer(); err == nil {
				t.Fatal("interrupting the session must reach the pipe")
			}
		}
	}
}
