package conf

import "github.com/xtls/xray-core/common/protocol"

// UserRuntimeFields are the per-client runtime keys every protocol's client JSON
// shares with protocol.User; embedding keeps boot config and AddUser in step.
type UserRuntimeFields struct {
	UploadBandwidthBps    uint64 `json:"upload_bandwidth_bps"`
	UploadPeakBps         uint64 `json:"upload_peak_bps"`
	UploadBurstBytes      uint64 `json:"upload_burst_bytes"`
	DownloadBandwidthBps  uint64 `json:"download_bandwidth_bps"`
	DownloadPeakBps       uint64 `json:"download_peak_bps"`
	DownloadBurstBytes    uint64 `json:"download_burst_bytes"`
	BurstBitPerSec        uint64 `json:"burst_bit_per_sec"`
	BurstCreditBytes      uint64 `json:"burst_credit_bytes"`
	SustainedBitPerSec    uint64 `json:"sustained_bit_per_sec"`
	SustainedAfterSeconds uint32 `json:"sustained_after_seconds"`
	EgressTag             string `json:"egress_tag"`
}

func (f UserRuntimeFields) applyTo(u *protocol.User) *protocol.User {
	u.UploadBandwidthBps, u.UploadPeakBps, u.UploadBurstBytes = f.UploadBandwidthBps, f.UploadPeakBps, f.UploadBurstBytes
	u.DownloadBandwidthBps, u.DownloadPeakBps, u.DownloadBurstBytes = f.DownloadBandwidthBps, f.DownloadPeakBps, f.DownloadBurstBytes
	u.BurstBitPerSec, u.BurstCreditBytes = f.BurstBitPerSec, f.BurstCreditBytes
	u.SustainedBitPerSec, u.SustainedAfterSeconds = f.SustainedBitPerSec, f.SustainedAfterSeconds
	u.EgressTag = f.EgressTag
	return u
}
