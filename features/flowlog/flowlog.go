// Package flowlog 是 dispatcher 看到的审计挂载点：每条连接开一条记录，结束时收尾。
// 记录写到哪、怎么轮转，留在 app/flowlog 里；没注册时 dispatcher 与上游一致。
package flowlog

import (
	"context"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features"
)

// Session 是一条连接开始时已知的事实。Outbound 在连接期间才会被回填拨号地址，所以传指针、记录时再读。
type Session struct {
	Email      string
	InboundTag string
	Source     net.Destination
	Target     net.Destination
	Outbound   *session.Outbound
}

type WrapReader func(buf.Reader) buf.Reader
type WrapWriter func(buf.Writer) buf.Writer

// Recorder 由 app/flowlog 实现。
type Recorder interface {
	features.Feature
	// Open 开始记录一条连接：返回给上行（Reader）与下行（Writer）计字节的包装，以及收尾回调。
	// 收尾时 result 为空表示正常结束，否则是拒绝或失败的原因。
	Open(ctx context.Context, s Session) (WrapReader, WrapWriter, func(result string))
}

func Type() interface{} { return (*Recorder)(nil) }
