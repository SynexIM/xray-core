package command

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/serial"
	featureoutbound "github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
)

type rateLimitOutbound struct {
	rateLimitBitPerSec uint64
}

func (*rateLimitOutbound) Start() error { return nil }
func (*rateLimitOutbound) Close() error { return nil }
func (*rateLimitOutbound) Tag() string  { return "test-outbound" }
func (*rateLimitOutbound) Dispatch(context.Context, *transport.Link) {
}
func (*rateLimitOutbound) SenderSettings() *serial.TypedMessage { return nil }
func (*rateLimitOutbound) ProxySettings() *serial.TypedMessage  { return nil }
func (o *rateLimitOutbound) SetOutboundRateLimitBitPerSec(value uint64) error {
	o.rateLimitBitPerSec = value
	return nil
}

type rateLimitOutboundManager struct {
	handler featureoutbound.Handler
}

func (*rateLimitOutboundManager) Start() error      { return nil }
func (*rateLimitOutboundManager) Close() error      { return nil }
func (*rateLimitOutboundManager) Type() interface{} { return featureoutbound.ManagerType() }
func (m *rateLimitOutboundManager) GetHandler(tag string) featureoutbound.Handler {
	if m.handler != nil && m.handler.Tag() == tag {
		return m.handler
	}
	return nil
}
func (m *rateLimitOutboundManager) GetDefaultHandler() featureoutbound.Handler {
	return m.handler
}
func (m *rateLimitOutboundManager) AddHandler(context.Context, featureoutbound.Handler) error {
	return nil
}
func (m *rateLimitOutboundManager) RemoveHandler(context.Context, string) error { return nil }
func (m *rateLimitOutboundManager) ListHandlers(context.Context) []featureoutbound.Handler {
	return []featureoutbound.Handler{m.handler}
}

func TestSetOutboundRateLimitOperationMutatesLiveHandler(t *testing.T) {
	handler := &rateLimitOutbound{}
	op := &SetOutboundRateLimitOperation{RateLimitBitPerSec: 80_000_000}
	if err := op.ApplyOutbound(context.Background(), handler); err != nil {
		t.Fatalf("apply outbound rate limit: %v", err)
	}
	if got := handler.rateLimitBitPerSec; got != 80_000_000 {
		t.Fatalf("handler rate = %d bit/s, want 80000000", got)
	}
}

func TestAlterOutboundDecodesAndAppliesRateLimitOperation(t *testing.T) {
	handler := &rateLimitOutbound{}
	server := &handlerServer{ohm: &rateLimitOutboundManager{handler: handler}}
	_, err := server.AlterOutbound(context.Background(), &AlterOutboundRequest{
		Tag:       handler.Tag(),
		Operation: serial.ToTypedMessage(&SetOutboundRateLimitOperation{RateLimitBitPerSec: 80_000_000}),
	})
	if err != nil {
		t.Fatalf("AlterOutbound: %v", err)
	}
	if got := handler.rateLimitBitPerSec; got != 80_000_000 {
		t.Fatalf("handler rate = %d bit/s, want 80000000", got)
	}
}

func TestOutboundRateLimitProtoFieldsNameTheirUnits(t *testing.T) {
	operationField := (&SetOutboundRateLimitOperation{}).ProtoReflect().Descriptor().Fields().ByName("rate_limit_bit_per_sec")
	if operationField == nil {
		t.Fatal("SetOutboundRateLimitOperation must expose rate_limit_bit_per_sec")
	}
}
