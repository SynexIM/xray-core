package protocol_test

import (
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/common/protocol"
	"golang.org/x/time/rate"
)

// firstLimiter 取用户的第一个桶（单速率时就是唯一那个）。
func firstLimiter(u *MemoryUser) (*rate.Limiter, int) {
	limiters, burst := u.RuntimeRateLimiters(buf.NewRateLimiterWithBurst)
	if len(limiters) == 0 {
		return nil, burst
	}
	return limiters[0], burst
}

func TestRuntimeRateLimiterSharedPerUser(t *testing.T) {
	user := &MemoryUser{
		Email:        "alice@example.test",
		BandwidthBps: uint64(buf.Size),
	}

	limiter1, burst1 := firstLimiter(user)
	limiter2, burst2 := firstLimiter(user)
	if limiter1 == nil || limiter2 == nil {
		t.Fatal("expected limiter for bandwidth-limited user")
	}
	if limiter1 != limiter2 {
		t.Fatal("expected one shared limiter per MemoryUser")
	}
	if burst1 != burst2 {
		t.Fatalf("expected stable burst, got %d and %d", burst1, burst2)
	}
	if !limiter1.AllowN(time.Now(), burst1) {
		t.Fatal("expected initial burst to be available")
	}
	if limiter2.Allow() {
		t.Fatal("second access should consume the same exhausted bucket")
	}
}

func TestRuntimeRateLimiterIsolatedBetweenUsers(t *testing.T) {
	alice := &MemoryUser{
		Email:        "alice@example.test",
		BandwidthBps: uint64(buf.Size),
	}
	bob := &MemoryUser{
		Email:        "bob@example.test",
		BandwidthBps: uint64(buf.Size),
	}

	aliceLimiter, aliceBurst := firstLimiter(alice)
	bobLimiter, bobBurst := firstLimiter(bob)
	if aliceLimiter == nil || bobLimiter == nil {
		t.Fatal("expected limiters for both users")
	}
	if aliceLimiter == bobLimiter {
		t.Fatal("different users must not share limiter state")
	}
	if !aliceLimiter.AllowN(time.Now(), aliceBurst) {
		t.Fatal("expected alice initial burst to be available")
	}
	if !bobLimiter.AllowN(time.Now(), bobBurst) {
		t.Fatal("bob bucket should be independent from alice")
	}
}

func TestRuntimeRateLimiterConvertsBitsToBytes(t *testing.T) {
	user := &MemoryUser{
		Email:        "alice@example.test",
		BandwidthBps: 8_000_000,
	}
	var got uint64
	_, _ = user.RuntimeRateLimiters(func(bytesPerSecond, burstBytes uint64) (*rate.Limiter, int) {
		got = bytesPerSecond
		return buf.NewRateLimiterWithBurst(bytesPerSecond, burstBytes)
	})
	if got != 1_000_000 {
		t.Fatalf("runtime limiter bytes/sec = %d, want 1000000", got)
	}
}

func TestResetRuntimeLimiter(t *testing.T) {
	user := &MemoryUser{
		Email:        "alice@example.test",
		BandwidthBps: uint64(buf.Size),
	}

	limiter1, _ := firstLimiter(user)
	user.ResetRuntimeLimiter()
	limiter2, _ := firstLimiter(user)
	if limiter1 == nil || limiter2 == nil {
		t.Fatal("expected limiter before and after reset")
	}
	if limiter1 == limiter2 {
		t.Fatal("expected reset to drop old limiter state")
	}
}
