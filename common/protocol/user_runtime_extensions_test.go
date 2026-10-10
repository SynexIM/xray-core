package protocol_test

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/socks"
)

func TestUserRuntimeExtensionsRoundTrip(t *testing.T) {
	want := &MemoryUser{
		Email:                "client-uid",
		Level:                3,
		BandwidthBps:         90_000_000,
		CommittedBps:         30_000_000,
		CommittedBurstBytes:  1234,
		Class:                "live",
		ConnLimit:            7,
		UploadBandwidthBps:   8_000_000,
		UploadPeakBps:        12_000_000,
		UploadBurstBytes:     4321,
		DownloadBandwidthBps: 16_000_000,
		DownloadPeakBps:      24_000_000,
		DownloadBurstBytes:   8765,
		EgressTag:            "dedicated-exit",
	}
	protoUser := ToProtoUser(want)
	if protoUser.GetUploadBandwidthBps() != want.UploadBandwidthBps ||
		protoUser.GetDownloadPeakBps() != want.DownloadPeakBps ||
		protoUser.GetEgressTag() != want.EgressTag {
		t.Fatalf("runtime extension fields were not serialized: %+v", protoUser)
	}

	protoUser.Account = serial.ToTypedMessage(&socks.Account{Username: "u", Password: "p"})
	got, err := protoUser.ToMemoryUser()
	if err != nil {
		t.Fatal(err)
	}
	if got.UploadBandwidthBps != want.UploadBandwidthBps ||
		got.UploadPeakBps != want.UploadPeakBps ||
		got.UploadBurstBytes != want.UploadBurstBytes ||
		got.DownloadBandwidthBps != want.DownloadBandwidthBps ||
		got.DownloadPeakBps != want.DownloadPeakBps ||
		got.DownloadBurstBytes != want.DownloadBurstBytes ||
		got.EgressTag != want.EgressTag {
		t.Fatalf("runtime extension fields were not restored: %+v", got)
	}
}

func TestDirectionalLimitersAreIsolated(t *testing.T) {
	user := &MemoryUser{
		UploadBandwidthBps:   uint64(buf.Size) * 8,
		DownloadBandwidthBps: uint64(buf.Size) * 16,
	}
	t.Cleanup(user.ResetRuntimeLimiter)
	limits := user.RuntimeDirectionalRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limits.Upload) != 1 || len(limits.Download) != 1 {
		t.Fatalf("directional limiter counts = upload %d, download %d", len(limits.Upload), len(limits.Download))
	}
	if limits.Upload[0] == limits.Download[0] {
		t.Fatal("upload and download must not share directional tokens")
	}
	now := time.Now()
	if !limits.Upload[0].AllowN(now, limits.Upload[0].Burst()) {
		t.Fatal("upload bucket should start full")
	}
	if !limits.Download[0].AllowN(now, limits.Download[0].Burst()) {
		t.Fatal("consuming upload tokens must not consume download tokens")
	}
	if got, want := limits.Upload[0].Limit(), limits.Download[0].Limit()/2; got != want {
		t.Fatalf("upload limit = %v, want %v", got, want)
	}
}

func TestSymmetricLimitersRemainSharedWithoutDirectionalFields(t *testing.T) {
	user := &MemoryUser{BandwidthBps: uint64(buf.Size) * 8, CommittedBps: uint64(buf.Size) * 4}
	t.Cleanup(user.ResetRuntimeLimiter)
	limits := user.RuntimeDirectionalRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limits.Upload) != 2 || len(limits.Download) != 2 {
		t.Fatalf("symmetric limiter counts = upload %d, download %d", len(limits.Upload), len(limits.Download))
	}
	if limits.Upload[0] != limits.Download[0] || limits.Upload[1] != limits.Download[1] {
		t.Fatal("legacy symmetric policy must retain shared-token behavior")
	}
}
