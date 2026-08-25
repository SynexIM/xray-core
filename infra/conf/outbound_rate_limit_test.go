package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/infra/conf"
)

func TestOutboundRateLimitBuildsIntoSenderConfig(t *testing.T) {
	var raw conf.OutboundDetourConfig
	if err := json.Unmarshal([]byte(`{
		"tag": "shared-egress",
		"protocol": "freedom",
		"rateLimitBitPerSec": 80000000
	}`), &raw); err != nil {
		t.Fatalf("parse outbound config: %v", err)
	}
	built, err := raw.Build()
	if err != nil {
		t.Fatalf("build outbound config: %v", err)
	}
	instance, err := built.SenderSettings.GetInstance()
	if err != nil {
		t.Fatalf("decode sender settings: %v", err)
	}
	sender, ok := instance.(*proxyman.SenderConfig)
	if !ok {
		t.Fatalf("sender settings type = %T", instance)
	}
	if got := sender.GetRateLimitBitPerSec(); got != 80_000_000 {
		t.Fatalf("sender rate = %d bit/s, want 80000000", got)
	}
	if sender.RateLimitBitPerSec == nil {
		t.Fatal("an explicit rateLimitBitPerSec must preserve field presence for hot updates")
	}
	field := sender.ProtoReflect().Descriptor().Fields().ByName("rate_limit_bit_per_sec")
	if field == nil {
		t.Fatal("SenderConfig must expose rate_limit_bit_per_sec")
	}
	if !field.HasPresence() {
		t.Fatal("rate_limit_bit_per_sec must distinguish omitted from explicit zero")
	}
}

func TestOutboundWithoutRateLimitKeepsFieldAbsent(t *testing.T) {
	raw := conf.OutboundDetourConfig{Protocol: "freedom", Tag: "ordinary-egress"}
	built, err := raw.Build()
	if err != nil {
		t.Fatalf("build outbound config: %v", err)
	}
	instance, err := built.SenderSettings.GetInstance()
	if err != nil {
		t.Fatalf("decode sender settings: %v", err)
	}
	sender := instance.(*proxyman.SenderConfig)
	if sender.RateLimitBitPerSec != nil {
		t.Fatalf("ordinary outbound unexpectedly opted into hot rate limiting: %v", *sender.RateLimitBitPerSec)
	}
}
