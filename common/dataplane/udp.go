// Package dataplane stores process-wide transport counters, including listeners
// that are shared by multiple Xray instances during an in-process handover.
package dataplane

import (
	"sync/atomic"

	"github.com/xtls/xray-core/features/stats"
)

const (
	UDPPipeBytes         = 256 * 1024
	UDPHubPackets        = 1024
	UDPDispatcherPackets = 256
	UDPWireGuardPackets  = 1024
	UDPHysteriaPackets   = 1024
)

type UDPLayer uint8

const (
	UDPPipe UDPLayer = iota
	UDPHub
	UDPDispatcher
	UDPWireGuard
	UDPHysteria
)

type counter struct{ value atomic.Int64 }

func (counter *counter) Value() int64          { return counter.value.Load() }
func (counter *counter) Set(value int64) int64 { return counter.value.Swap(value) }
func (counter *counter) Add(value int64) int64 { return counter.value.Add(value) }

var udpDrops [5]counter
var udpDropNames = [5]string{
	"dataplane>>>udp_drop>>>pipe",
	"dataplane>>>udp_drop>>>hub",
	"dataplane>>>udp_drop>>>dispatcher",
	"dataplane>>>udp_drop>>>wireguard",
	"dataplane>>>udp_drop>>>hysteria",
}

func DropUDP(layer UDPLayer, packets int64) { udpDrops[layer].Add(packets) }

func Counter(name string) stats.Counter {
	for layer, candidate := range udpDropNames {
		if candidate == name {
			return &udpDrops[layer]
		}
	}
	return nil
}

func VisitCounters(visitor func(string, stats.Counter) bool) bool {
	for layer, name := range udpDropNames {
		if !visitor(name, &udpDrops[layer]) {
			return false
		}
	}
	return true
}
