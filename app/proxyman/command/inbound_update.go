package command

import (
	"context"

	"github.com/xtls/xray-core/app/capability"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/inbound"
	"google.golang.org/protobuf/proto"
)

func (op *UpdateInboundOperation) ApplyInbound(_ context.Context, handler inbound.Handler) error {
	if op.Inbound == nil || op.Inbound.Tag != handler.Tag() {
		return errors.New("inbound update needs the existing tag")
	}
	if !proto.Equal(handler.ProxySettings(), op.Inbound.ProxySettings) {
		return errors.New("inbound protocol/authentication changes require rebuilding")
	}
	settings, err := op.Inbound.ReceiverSettings.GetInstance()
	if err != nil {
		return err
	}
	receiver, ok := settings.(*proxyman.ReceiverConfig)
	if !ok {
		return errors.New("not a ReceiverConfig")
	}
	updater, ok := handler.(interface {
		UpdateReceiver(*proxyman.ReceiverConfig) error
	})
	if !ok {
		return errors.New("handler does not support receiver updates")
	}
	return updater.UpdateReceiver(receiver)
}

func init() { capability.Register("inbound.hotReload") }
