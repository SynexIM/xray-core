package limit

import (
	"sort"
	"time"
)

const (
	heavyBuckets             = 15
	fairFillMaxRounds        = 8
	fairSatisfiedHeadroomDiv = 8
)

type NodeStatus struct {
	Congested              bool
	UtilizationPercent     float64
	UploadBps, DownloadBps uint64
}

func (r *Runtime) NodeStatus() NodeStatus {
	r.nodeMu.RLock()
	defer r.nodeMu.RUnlock()
	return r.nodeStatus
}

type congestionState struct {
	congested  bool
	belowSince time.Time
}

func updateCongestion(state *congestionState, n *NodeLimit, used float64, now time.Time) bool {
	if n == nil || n.CapacityBps == 0 || n.EnterPercent == 0 {
		*state = congestionState{}
		return false
	}
	util := used * 800 / float64(n.CapacityBps)
	if !state.congested {
		if util >= float64(n.EnterPercent) {
			state.congested = true
		}
		return state.congested
	}
	if util <= float64(n.ExitPercent) {
		if state.belowSince.IsZero() {
			state.belowSince = now
		}
		if now.Sub(state.belowSince) >= time.Duration(n.HoldSeconds)*time.Second {
			*state = congestionState{}
		}
	} else {
		state.belowSince = time.Time{}
	}
	return state.congested
}

type bucketUsage struct{ up, down directionUsage }
type directionUsage struct {
	lastGranted, lastWaits uint64
	usage                  usageWindow
}
type usageWindow struct {
	window, span uint32
	count, idx   int
	ring         [heavyBuckets]float64
	cur          float64
	ticks        uint32
}

func (w *usageWindow) add(delta float64, window uint32) {
	if window == 0 {
		*w = usageWindow{}
		return
	}
	if w.window != window {
		span := uint32((uint64(window) + heavyBuckets - 1) / heavyBuckets)
		count := int((uint64(window) + uint64(span) - 1) / uint64(span))
		*w = usageWindow{window: window, count: count, span: span}
	}
	w.cur += delta
	w.ticks++
	if w.ticks >= w.span {
		w.ring[w.idx] = w.cur
		w.idx = (w.idx + 1) % w.count
		w.cur = 0
		w.ticks = 0
	}
}
func (w *usageWindow) average() float64 {
	if w.window == 0 {
		return 0
	}
	sum := w.cur
	remaining := w.window - w.ticks
	full := int(remaining / w.span)
	for i := 0; i < full; i++ {
		sum += w.ring[(w.idx-1-i+w.count)%w.count]
	}
	if partial := remaining % w.span; partial > 0 {
		sum += w.ring[(w.idx-1-full+w.count)%w.count] * float64(partial) / float64(w.span)
	}
	return sum / float64(w.window)
}

type fairSlot struct {
	key                                 string
	shaper                              *Shaper
	delta                               float64
	blocked, heavy, pinned              bool
	weight, ceiling, floor, want, alloc float64
}
type fillOutcome struct {
	truncated          bool
	rounds, unresolved int
}

// 加权注水沿用旧执行器；取消 class 聚合保底和耐力，按 profile 保底。
func fill(slots []*fairSlot, root float64) fillOutcome {
	sort.Slice(slots, func(i, j int) bool { return slots[i].key < slots[j].key })
	remaining := root
	for _, layer := range []bool{false, true} {
		extra := 0.0
		for _, m := range slots {
			if m.heavy == layer {
				extra += m.floor
			}
		}
		if extra > remaining {
			for _, m := range slots {
				if m.heavy == layer {
					m.floor = 0
				}
			}
		} else {
			remaining -= extra
		}
	}
	var normal, heavy []*fairSlot
	for _, m := range slots {
		m.alloc = m.floor
		m.want = max(m.want, m.floor)
		if m.heavy {
			heavy = append(heavy, m)
		} else {
			normal = append(normal, m)
		}
	}
	remaining, normalOut, normalConstrained, normalLevel := waterFill(normal, remaining)
	_, heavyOut, heavyConstrained, heavyLevel := waterFill(heavy, remaining)
	if !normalConstrained && !heavyConstrained {
		for _, m := range slots {
			m.alloc = m.ceiling
		}
	} else {
		raise := func(members []*fairSlot, level float64) {
			for _, m := range members {
				if !m.blocked && level > 0 {
					m.alloc = max(m.alloc, min(m.ceiling, m.floor+level*m.weight))
				}
			}
		}
		raise(normal, normalLevel)
		raise(heavy, heavyLevel)
	}
	return fillOutcome{truncated: normalOut.truncated || heavyOut.truncated, rounds: max(normalOut.rounds, heavyOut.rounds), unresolved: normalOut.unresolved + heavyOut.unresolved}
}

// waterFill 在 members 之间按 weight 分 pool（每人已有 alloc=floor、want≥floor），返回剩余。
func waterFill(members []*fairSlot, pool float64) (float64, fillOutcome, bool, float64) {
	out := fillOutcome{}
	for round := 0; ; round++ {
		out.rounds = round
		var totalWeight float64
		out.unresolved = 0
		for _, m := range members {
			if !m.pinned {
				totalWeight += m.weight
				out.unresolved++
			}
		}
		if totalWeight == 0 {
			out.unresolved = 0
			return pool, out, false, 0
		}
		if round >= fairFillMaxRounds {
			splitRemainder(members, pool, totalWeight)
			out.truncated = true
			return 0, out, true, pool / totalWeight
		}
		// 一轮之内所有人对着同一个 (pool, totalWeight) 判定，钉住后再一起扣。
		progressed := false
		var consumed float64
		for _, m := range members {
			if m.pinned {
				continue
			}
			if share := m.floor + pool*m.weight/totalWeight; m.want <= share {
				m.alloc, m.pinned, progressed = m.want, true, true
				consumed += m.want - m.floor
			}
		}
		pool -= consumed
		if !progressed {
			splitRemainder(members, pool, totalWeight)
			out.unresolved = 0
			return 0, out, true, pool / totalWeight
		}
	}
}

func splitRemainder(members []*fairSlot, pool, totalWeight float64) {
	for _, m := range members {
		if !m.pinned {
			m.alloc = m.floor + pool*m.weight/totalWeight
			m.pinned = true
		}
	}
}

func (r *Runtime) direction(buckets []*bucket, root, available, seconds float64, upload, congested bool) {
	slots := make([]*fairSlot, 0, len(buckets))
	for _, b := range buckets {
		shaper, state := b.down, &b.fair.down
		if upload {
			shaper, state = b.up, &b.fair.up
		}
		granted, waits, rate := shaper.sample()
		delta := float64(granted-state.lastGranted) / seconds
		blocked := waits != state.lastWaits
		state.lastGranted = granted
		state.lastWaits = waits
		p := b.profile.current.Load()
		c := p.Congested
		if c == nil {
			shaper.setShare(false, 0)
			continue
		}
		state.usage.add(delta, c.HeavyWindowSeconds)
		if !congested {
			shaper.setShare(false, 0)
			continue
		}
		if delta == 0 && !blocked {
			shaper.setShare(false, 0)
			continue
		}
		ceiling := root
		if rate > 0 {
			ceiling = min(ceiling, rate)
		}
		want := ceiling
		if !blocked {
			want = min(ceiling, delta+delta/fairSatisfiedHeadroomDiv)
		}
		weight := float64(c.Weight)
		if weight == 0 {
			weight = 1
		}
		slot := &fairSlot{key: b.tag, shaper: shaper, delta: delta, blocked: blocked, weight: weight, ceiling: ceiling, want: want, floor: min(float64(c.FloorBps)/8, want)}
		slot.heavy = c.HeavyWindowSeconds > 0 && c.HeavyPercent > 0 && rate > 0 && state.usage.average()*100 >= rate*float64(c.HeavyPercent)
		slots = append(slots, slot)
	}
	if !congested {
		return
	}
	fill(slots, available)
	for _, slot := range slots {
		// 零份额仍然受限；整形器用显式拥挤标记区分它和无限速。
		slot.shaper.setShare(true, slot.alloc)
	}
}
func (r *Runtime) tick(now time.Time) {
	r.tickMu.Lock()
	defer r.tickMu.Unlock()
	buckets := r.activeBuckets()
	r.mu.RLock()
	node := r.node
	r.mu.RUnlock()

	seconds := now.Sub(r.lastTick).Seconds()
	if seconds <= 0 {
		return
	}
	r.lastTick = now
	up, down := r.upBytes.Load(), r.downBytes.Load()
	upRate, downRate := float64(up-r.lastUp)/seconds, float64(down-r.lastDown)/seconds
	r.lastUp = up
	r.lastDown = down
	participatingUp, participatingDown := r.participatingUp.Load(), r.participatingDown.Load()
	eligibleUp := float64(participatingUp-r.lastParticipatingUp) / seconds
	eligibleDown := float64(participatingDown-r.lastParticipatingDown) / seconds
	r.lastParticipatingUp = participatingUp
	r.lastParticipatingDown = participatingDown
	upCongested := updateCongestion(&r.congestion[0], node, upRate, now)
	downCongested := updateCongestion(&r.congestion[1], node, downRate, now)
	var root, util float64
	if node != nil {
		root = float64(node.CapacityBps) / 8
	}
	if root > 0 {
		util = max(upRate, downRate) * 100 / root
	}
	r.nodeMu.Lock()
	r.nodeStatus = NodeStatus{Congested: upCongested || downCongested, UtilizationPercent: util, UploadBps: uint64(upRate * 8), DownloadBps: uint64(downRate * 8)}
	r.nodeMu.Unlock()
	// 不参与注水的流量仍占容量；只在剩余带宽里分份额，重叠规则不能重复扣除用量。
	r.direction(buckets, root, max(0, root-max(0, upRate-eligibleUp)), seconds, true, upCongested)
	r.direction(buckets, root, max(0, root-max(0, downRate-eligibleDown)), seconds, false, downCongested)
	r.removeIdleActive(buckets)
	r.collectRetired(now)
}
