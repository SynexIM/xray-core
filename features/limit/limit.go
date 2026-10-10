package limit

import (
	"context"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features"
)

type SessionInfo struct {
	Email, InboundTag, OutboundTag, SourceIP string
	Datagram                                 bool
}
type WrapReader func(buf.Reader) buf.Reader
type WrapWriter func(buf.Writer) buf.Writer

// dispatcher 只认挂载接口，规则与整形算法留在 app 内。
type Limiter interface {
	features.Feature
	Attach(context.Context, SessionInfo) (WrapReader, WrapWriter, func(), error)
}

func Type() interface{} { return (*Limiter)(nil) }
