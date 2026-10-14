package protocol

import (
	"errors"
	"github.com/xtls/xray-core/features/stats"
	"sync/atomic"
)

var ErrStatsIdentityRevoked = errors.New("stats: 身份已撤销")

type userStats struct {
	counters stats.UserCounters
	released atomic.Bool
}

func (user *MemoryUser) PrepareStats(registry stats.UserRegistry, uplink, downlink, online bool) error {
	if user == nil || user.Email == "" {
		return nil
	}
	if current := user.runtimeStats.Load(); current != nil {
		if current.released.Load() {
			return ErrStatsIdentityRevoked
		}
		current.counters.Enable(uplink, downlink, online)
		return nil
	}
	counters, err := registry.AcquireUser(user.Email, uplink, downlink, online)
	if err != nil {
		return err
	}
	next := &userStats{counters: counters}
	if !user.runtimeStats.CompareAndSwap(nil, next) {
		counters.Release()
		return user.PrepareStats(registry, uplink, downlink, online)
	}
	return nil
}
func (user *MemoryUser) Stats() stats.UserCounters {
	if user == nil {
		return nil
	}
	state := user.runtimeStats.Load()
	if state == nil {
		return nil
	}
	return state.counters
}
func (user *MemoryUser) ReleaseStats() {
	if user == nil {
		return
	}
	state := user.runtimeStats.Load()
	if state != nil && state.released.CompareAndSwap(false, true) {
		state.counters.Release()
	}
}
func (user *MemoryUser) shareStats(previous *MemoryUser) {
	user.runtimeStats.Store(previous.runtimeStats.Load())
}
