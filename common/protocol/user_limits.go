package protocol

import (
	"sync"

	"golang.org/x/time/rate"
)

type runtimeLimiterState struct {
	mu sync.Mutex
	// Symmetric state preserves the existing PIR/CIR/CBS implementation.
	bps      uint64
	cir      uint64
	cbs      uint64
	limiters []*rate.Limiter
	burst    int

	// Directional state is used only when any directional field is present.
	upload   runtimeDirectionalLimiterState
	download runtimeDirectionalLimiterState
}

type runtimeDirectionalLimiterState struct {
	bandwidthBps uint64
	peakBps      uint64
	burstBytes   uint64
	limiters     []*rate.Limiter
}

// DirectionalRateLimiters is the one runtime-shaping seam used by dispatcher.
// With no directional fields both directions intentionally receive the same
// symmetric PIR/CIR/CBS buckets, preserving existing shared-token semantics.
type DirectionalRateLimiters struct {
	Upload   []*rate.Limiter
	Download []*rate.Limiter
}

var runtimeLimiters sync.Map

// RuntimeLimits returns the maximum user ceiling and connection cap. Zero
// values mean unlimited and preserve upstream behavior.
func (u *MemoryUser) RuntimeLimits() (bandwidthBps uint64, connLimit uint32) {
	if u == nil {
		return 0, 0
	}
	return u.runtimeCeilingBps(), u.ConnLimit
}

// HasRuntimeLimits reports whether the dispatcher must avoid the splice fast
// path because any runtime shaper or connection cap is active.
func (u *MemoryUser) HasRuntimeLimits() bool {
	if u == nil {
		return false
	}
	return u.BandwidthBps != 0 || u.ConnLimit != 0 || u.CommittedBps != 0 ||
		u.UploadBandwidthBps != 0 || u.UploadPeakBps != 0 ||
		u.DownloadBandwidthBps != 0 || u.DownloadPeakBps != 0 ||
		u.BurstBitPerSec != 0 || u.SustainedBitPerSec != 0 || u.HasTierSeam()
}

// RuntimeRateLimiters returns the user's shared token buckets, in the order the
// bytes must pass through them (loose → tight). All concurrent links for the
// same MemoryUser consume from these same buckets.
//
// 返回几个桶，取决于配置：
//
//	CIR = 0                 → 一个桶（峰值桶，跑 PIR）。就是改动前的单速率行为。
//	0 < CIR < PIR           → 两个桶：峰值桶 + 更深的承诺桶。新连接先跑 PIR，
//	                          CBS 花完后自然落到 CIR。
//	CIR >= PIR（PIR > 0）    → 串一个不比峰值更紧的桶毫无意义，只会平白多一次
//	                          WaitN，所以忽略 CIR，退化成单速率。
//	CBS = 0                 → 承诺桶没有额度（默认 1/8 秒窗口），等于单速率 CIR。
//	PIR = 0 且 CIR > 0      → 只有承诺桶，按**单速率 CIR** 处理（默认 1/8 秒窗口，
//	                          CBS 忽略）。理由：CBS 的定义是「能以峰值速率花掉多少」，
//	                          没有峰值速率时它无处可花；若照搬 CBS 当 burst，
//	                          一个只填了 CIR 的用户会先获得几十 GB 的不限速额度——
//	                          配错的后果应该是「限住了」，不是「放开了」。
//
// BandwidthBps / CommittedBps 是业务单位 bit/s，限速器吃 byte/s，转换只在这里做。
//
// newLimiter 的第二个参数是 burst 字节数，0 表示用默认的 1/8 秒窗口
// （见 buf.NewRateLimiterWithBurst）。
func (u *MemoryUser) RuntimeRateLimiters(newLimiter func(bytesPerSecond, burstBytes uint64) (*rate.Limiter, int)) ([]*rate.Limiter, int) {
	if u == nil || (u.BandwidthBps == 0 && u.CommittedBps == 0) {
		return nil, 0
	}
	// Fast path: avoid allocating a fresh state on every connection. LoadOrStore
	// eagerly evaluates its value argument, so a plain Load hit skips the alloc.
	raw, ok := runtimeLimiters.Load(u)
	if !ok {
		raw, _ = runtimeLimiters.LoadOrStore(u, new(runtimeLimiterState))
	}
	state := raw.(*runtimeLimiterState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.limiters == nil ||
		state.bps != u.BandwidthBps ||
		state.cir != u.CommittedBps ||
		state.cbs != u.CommittedBurstBytes {
		state.limiters, state.burst = buildRuntimeLimiters(u, newLimiter)
		state.bps = u.BandwidthBps
		state.cir = u.CommittedBps
		state.cbs = u.CommittedBurstBytes
	}
	return state.limiters, state.burst
}

// RuntimeDirectionalRateLimiters returns the per-direction buckets dispatcher
// must apply. Directional configuration uses independent legacy
// committed/peak/burst buckets; otherwise both directions share the existing
// symmetric PIR/CIR/CBS bucket chain exactly as before.
func (u *MemoryUser) RuntimeDirectionalRateLimiters(newLimiter func(uint64, uint64) (*rate.Limiter, int)) DirectionalRateLimiters {
	if u == nil {
		return DirectionalRateLimiters{}
	}
	if !u.hasDirectionalLimits() {
		limiters, _ := u.RuntimeRateLimiters(newLimiter)
		return DirectionalRateLimiters{Upload: limiters, Download: limiters}
	}
	raw, ok := runtimeLimiters.Load(u)
	if !ok {
		raw, _ = runtimeLimiters.LoadOrStore(u, new(runtimeLimiterState))
	}
	state := raw.(*runtimeLimiterState)
	state.mu.Lock()
	defer state.mu.Unlock()
	return DirectionalRateLimiters{
		Upload:   state.upload.update(u.UploadBandwidthBps, u.UploadPeakBps, u.UploadBurstBytes, newLimiter),
		Download: state.download.update(u.DownloadBandwidthBps, u.DownloadPeakBps, u.DownloadBurstBytes, newLimiter),
	}
}

func (s *runtimeDirectionalLimiterState) update(bandwidthBps, peakBps, burstBytes uint64, newLimiter func(uint64, uint64) (*rate.Limiter, int)) []*rate.Limiter {
	if s.limiters == nil || s.bandwidthBps != bandwidthBps || s.peakBps != peakBps || s.burstBytes != burstBytes {
		s.limiters = buildDirectionalRuntimeLimiters(bandwidthBps, peakBps, burstBytes, newLimiter)
		s.bandwidthBps, s.peakBps, s.burstBytes = bandwidthBps, peakBps, burstBytes
	}
	return s.limiters
}

func buildDirectionalRuntimeLimiters(bandwidthBps, peakBps, burstBytes uint64, newLimiter func(uint64, uint64) (*rate.Limiter, int)) []*rate.Limiter {
	committed := bitsPerSecondToRuntimeBytesPerSecond(bandwidthBps)
	peak := bitsPerSecondToRuntimeBytesPerSecond(peakBps)
	if committed == 0 {
		if peak == 0 {
			return nil
		}
		limiter, _ := newLimiter(peak, 0)
		return []*rate.Limiter{limiter}
	}
	committedLimiter, _ := newLimiter(committed, burstBytes)
	if peak == 0 {
		return []*rate.Limiter{committedLimiter}
	}
	peakLimiter, _ := newLimiter(peak, 0)
	return []*rate.Limiter{peakLimiter, committedLimiter}
}

func (u *MemoryUser) hasDirectionalLimits() bool {
	return u.UploadBandwidthBps != 0 || u.UploadPeakBps != 0 || u.UploadBurstBytes != 0 ||
		u.DownloadBandwidthBps != 0 || u.DownloadPeakBps != 0 || u.DownloadBurstBytes != 0
}

func (u *MemoryUser) runtimeCeilingBps() uint64 {
	if u == nil {
		return 0
	}
	if u.UsesTierShaping() {
		up, down := u.TierPolicies()
		return 8 * max64(max64(up.Standard, up.Burst), max64(down.Standard, down.Burst))
	}
	if !u.hasDirectionalLimits() {
		if u.BandwidthBps != 0 {
			return u.BandwidthBps
		}
		return u.CommittedBps
	}
	return max64(directionCeilingBps(u.UploadBandwidthBps, u.UploadPeakBps), directionCeilingBps(u.DownloadBandwidthBps, u.DownloadPeakBps))
}

func directionCeilingBps(bandwidthBps, peakBps uint64) uint64 {
	if peakBps != 0 {
		return peakBps
	}
	return bandwidthBps
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// buildRuntimeLimiters 把三个业务字段翻成一串桶。返回的 int 是**第一个桶**的
// burst（单速率时就是唯一那个桶的），调用方只拿它做诊断与测试断言。
func buildRuntimeLimiters(u *MemoryUser, newLimiter func(bytesPerSecond, burstBytes uint64) (*rate.Limiter, int)) ([]*rate.Limiter, int) {
	peak := bitsPerSecondToRuntimeBytesPerSecond(u.BandwidthBps)
	committed := bitsPerSecondToRuntimeBytesPerSecond(u.CommittedBps)

	// PIR 未设：只有承诺桶，当单速率 CIR 处理（CBS 无处可花，忽略）。
	if peak == 0 {
		limiter, burst := newLimiter(committed, 0)
		return []*rate.Limiter{limiter}, burst
	}

	peakLimiter, peakBurst := newLimiter(peak, 0)
	// CIR 未设，或不比 PIR 松——单速率。
	if committed == 0 || committed >= peak {
		return []*rate.Limiter{peakLimiter}, peakBurst
	}

	// CBS = 0 就是没有额度：承诺桶只有默认的小窗口，等于单速率 CIR。
	// 这里不替控制面编一个「一天的承诺量」之类的默认额度。
	burstBytes := u.CommittedBurstBytes
	committedLimiter, _ := newLimiter(committed, burstBytes)
	return []*rate.Limiter{peakLimiter, committedLimiter}, peakBurst
}

func bitsPerSecondToRuntimeBytesPerSecond(bitsPerSecond uint64) uint64 {
	if bitsPerSecond == 0 {
		return 0
	}
	return (bitsPerSecond + 7) / 8
}

func (u *MemoryUser) ResetRuntimeLimiter() {
	if u == nil {
		return
	}
	runtimeLimiters.Delete(u)
}
