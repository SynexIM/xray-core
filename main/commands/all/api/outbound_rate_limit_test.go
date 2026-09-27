package api

import (
	"testing"

	handlerService "github.com/xtls/xray-core/app/proxyman/command"
)

func TestOutboundRateLimitRequestPreservesTagAndBitUnit(t *testing.T) {
	request := newOutboundRateLimitRequest("shared-egress", 80_000_000)
	if request.Tag != "shared-egress" {
		t.Fatalf("tag = %q", request.Tag)
	}
	instance, err := request.Operation.GetInstance()
	if err != nil {
		t.Fatalf("decode operation: %v", err)
	}
	operation, ok := instance.(*handlerService.SetOutboundRateLimitOperation)
	if !ok {
		t.Fatalf("operation type = %T", instance)
	}
	if got := operation.GetRateLimitBitPerSec(); got != 80_000_000 {
		t.Fatalf("rate = %d bit/s, want 80000000", got)
	}
}
