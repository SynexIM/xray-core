package limit

import (
	"math"
	"time"
)

func (shaper *Shaper) updateBulk(flow *Flow) {
	bulk := !shaper.sparse(flow, 256)
	if bulk == flow.bulk {
		return
	}
	flow.bulk = bulk
	if bulk {
		shaper.bulkFlows++
	} else {
		shaper.bulkFlows--
	}
	shaper.version++
}
func (shaper *Shaper) rateVersion() (float64, uint64) {
	shaper.mu.Lock()
	defer shaper.mu.Unlock()
	shaper.advance(time.Now())
	if shaper.rate == 0 && !shaper.congested {
		return math.Inf(1), shaper.version
	}
	return shaper.rate, shaper.version
}
func (shaper *Shaper) pacingRate(flow *Flow) float64 {
	shaper.mu.Lock()
	defer shaper.mu.Unlock()
	now := time.Now()
	shaper.advance(now)
	shaper.meter(flow, now)
	shaper.updateBulk(flow)
	if shaper.rate == 0 && !shaper.congested {
		return math.Inf(1)
	}
	if !flow.bulk {
		return shaper.rate
	}
	return shaper.rate / float64(max(shaper.bulkFlows, 1))
}
