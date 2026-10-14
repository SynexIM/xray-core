package inbound

import (
	"context"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy/vmess/encoding"
	"io"
	"runtime/pprof"
)

var authenticationLabels = pprof.Labels("protocol", "vmess", "phase", "authentication")

func decodeRequestHeader(ctx context.Context, server *encoding.ServerSession, reader io.Reader, drain bool) (*protocol.RequestHeader, error) {
	var request *protocol.RequestHeader
	var err error
	pprof.Do(ctx, authenticationLabels, func(context.Context) {
		request, err = server.DecodeRequestHeader(reader, drain, session.InboundFromContext(ctx).Source.Address.String())
	})
	return request, err
}
