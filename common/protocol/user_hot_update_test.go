package protocol

import (
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestHotUpdateKeepsIdentityAndPublishesDetachedSnapshot(t *testing.T) {
	u := &MemoryUser{Email: "hot@example.test", Pool: "hot-pool", UploadBandwidthBps: 8_000_000, DownloadBandwidthBps: 16_000_000}
	stopped := false
	release, ok := u.TrackRuntimeSession(func() { stopped = true })
	if !ok {
		t.Fatal("session rejected")
	}
	defer release()
	n := &MemoryUser{Email: u.Email, Pool: u.Pool, UploadBandwidthBps: 24_000_000, DownloadBandwidthBps: 32_000_000, BurstCreditBytes: 123, EgressTag: "eg-hot"}
	u.AdoptLimits(n)
	n.DownloadBandwidthBps = 0
	actual := ToProtoUser(u)
	if actual.DownloadBandwidthBps != 32_000_000 || actual.EgressTag != "eg-hot" || u.DownloadBandwidthBps != 16_000_000 || stopped {
		t.Fatal("hot update mutated the authentication object, aliased input or stopped a session")
	}
	up, down := u.TierPolicies()
	if up.Standard != 3_000_000 || down.Standard != 4_000_000 {
		t.Fatal("tier policy did not use current snapshot")
	}
	u.RevokeRuntimeSessions()
	if !stopped {
		t.Fatal("hot update lost original session lifetime")
	}
}

func TestHotUpdateConcurrentRuntimeReaders(t *testing.T) {
	u := &MemoryUser{Email: "race@example.test", Pool: "race-pool", UploadBandwidthBps: 8_000_000, DownloadBandwidthBps: 8_000_000}
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 500 {
				p := ToProtoUser(u)
				if p.UploadBandwidthBps != p.DownloadBandwidthBps {
					t.Error("torn runtime snapshot")
				}
				u.RuntimeLimits()
				u.HasRuntimeLimits()
				u.RuntimeDirectionalRateLimiters(buf.NewRateLimiterWithBurst)
				up, down, release := u.AcquireTierShapers()
				if up == nil || down == nil {
					t.Error("tier seam lost")
				}
				if release != nil {
					release()
				}
			}
		}()
	}
	for n := uint64(1); n <= 500; n++ {
		u.AdoptLimits(&MemoryUser{Email: u.Email, Pool: u.Pool, UploadBandwidthBps: n * 8_000_000, DownloadBandwidthBps: n * 8_000_000})
		ApplyTierPolicy(u)
	}
	workers.Wait()
	u.ResetRuntimeLimiter()
}
