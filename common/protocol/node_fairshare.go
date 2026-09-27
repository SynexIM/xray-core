package protocol

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// NodeFairScheduler 是节点级的拥塞门控：只在节点真的挤的时候，才替池整形器（tier_shaper.go）
// 决定各池跑多快。
//
//	不挤  什么都不管——每个池标准封顶、有额度就突发（整形器自己做）
//	挤    每秒一次 work-conserving 加权注水，结果直接写成各池整形器的速率：
//	        1 保底：class 聚合保底（reserved）→ 正常池地板（floor）→ 重度池保底（= 持续速率），
//	          每一层只在「这一层全部给得起」时发放，给不起就整层不给
//	        2 剩余带宽先按 weight 注给正常池，目标到标准
//	        3 还剩就按 weight 注给重度池，目标也到标准（重度只是排在后面，不是被封死）
//	        4 谁也没被压住 → 大家放回标准封顶
//
// 重度池：class 配了重度窗口与比例，且这个池在最近 window 秒的平均用量 ≥ 标准 × 比例。
// 窗口之前的时间按零算，所以刚开始跑满的人不会立刻被当成重度。0 = 不识别重度。
//
// 活跃判定：本 tick 有排过队 = backlogged（还想要更多，需求按标准算）；没排过队 = satisfied
// （需求 = 实测 + 1/8 余量）。只有活跃的池参与注水。
//
// 额度总量契约：只要进入约束态（有池被份额压住），排队池的份额加未排队池的实测需求 ≤ root_cap；
// 未排队的池份额抬到同层水位，这部分余量不预留（见 fill）。
//
// 所有参数由控制面下发，0 一律是「没有这一项」，这里不藏任何默认值。
type NodeFairScheduler struct {
	mu sync.Mutex // recompute 与 Status 的拥塞态

	// root_cap：节点整形上限（字节/秒）。0 = 不开节点级调度。
	rootCapBytePerSec atomic.Uint64

	// 拥塞滞回。enter=0 表示不做判定：只要开着就一直按拥塞处理。
	congestionEnterPercent atomic.Uint32
	congestionExitPercent  atomic.Uint32
	congestionExitTicks    atomic.Uint32

	classes atomic.Pointer[map[string]*ClassPolicy]

	congested      bool // mu，任一方向拥塞
	upCongestion   fairCongestionState
	downCongestion fairCongestionState

	// 运行态，原子量让 Status() 不抢 recompute 的锁。
	activeMembers      atomic.Uint32
	heavyMembers       atomic.Uint32
	usedUpload         atomic.Uint64
	usedDownload       atomic.Uint64
	fillTruncated      atomic.Bool
	fillRounds         atomic.Uint32
	fillUnresolved     atomic.Uint32
	fillTruncatedTicks atomic.Uint64
	fillTruncatedTotal atomic.Uint64

	started atomic.Bool
}

// ClassPolicy 是一组池共享的争抢参数，随 SetClassPolicies 整份下发。0 = 没有这一项。
type ClassPolicy struct {
	Name   string
	Weight uint32 // 0 与 1 等价：不加权

	// FloorBytePerSec：拥挤时每个活跃池的地板（不超过它自己的需求）。
	FloorBytePerSec uint64

	// Reserved 是该 class 全体活跃池共享的方向性保底，不是逐池保底；没人用的部分当 tick 回流。
	UploadReservedBytePerSec   uint64
	DownloadReservedBytePerSec uint64

	// 重度识别：最近 HeavyWindowSeconds 秒平均用量 ≥ 标准 × HeavyPercent% 的池，拥挤时排在后面。
	HeavyWindowSeconds uint32
	HeavyPercent       uint32
}

var nodeFairScheduler = &NodeFairScheduler{}

// FairScheduler 返回进程级单例（节点 = 单 xray 进程）。
func FairScheduler() *NodeFairScheduler { return nodeFairScheduler }

const (
	fairRecomputeEvery = time.Second

	// 活跃判定滞回：进入 > 4KB/tick（滤 keepalive）；退出需 < 1KB 且连续 3 tick。
	fairActiveEnterDeltaB = 4 * 1024
	fairActiveExitDeltaB  = 1 * 1024
	fairActiveExitTicks   = 3

	// satisfied 池的余量：钉在刚才的实测上会让它下一 tick 立刻撞桶，每秒来回抖。
	fairSatisfiedHeadroomDiv = 8

	// 注水轮数上限。正常两三轮收敛；截断后余下的按权重一次分完，并在 Status 里看得见。
	fairFillMaxRounds = 8

	// 截断持续时每 5 分钟复述一次日志。
	fairTruncationRestateTicks = 300

	// 重度窗口切成多少段滚动求和。段越多越准，内存按 段数 × 8B × 2 方向 / 池。
	heavyBuckets = 15
)

// SetNodeBandwidth 设置 root_cap（字节/秒）。0 = 关闭节点级调度。
func (s *NodeFairScheduler) SetNodeBandwidth(rootCapBytePerSec uint64) {
	s.rootCapBytePerSec.Store(rootCapBytePerSec)
	if rootCapBytePerSec > 0 {
		s.ensureStarted()
	}
}

// RootCapBytePerSec 返回当前节点整形上限。
func (s *NodeFairScheduler) RootCapBytePerSec() uint64 { return s.rootCapBytePerSec.Load() }

// Enabled 节点级调度是否开启（root_cap > 0）。
func (s *NodeFairScheduler) Enabled() bool { return s.rootCapBytePerSec.Load() > 0 }

// SetCongestionHysteresis 设置拥塞进出阈值（百分比）与退出所需的连续 tick 数。
func (s *NodeFairScheduler) SetCongestionHysteresis(enterPercent, exitPercent, exitTicks uint32) {
	s.congestionEnterPercent.Store(enterPercent)
	s.congestionExitPercent.Store(exitPercent)
	s.congestionExitTicks.Store(exitTicks)
}

// SetClassPolicies 整份替换 class 表（copy-on-write）。
func (s *NodeFairScheduler) SetClassPolicies(policies []*ClassPolicy) {
	byName := make(map[string]*ClassPolicy, len(policies))
	for _, p := range policies {
		if p != nil {
			cp := *p
			byName[cp.Name] = &cp
		}
	}
	s.classes.Store(&byName)
}

// ClassPolicyFor 返回名字精确匹配的 class；没有就是 nil（不加权、无地板、无保底、不识别重度）。
func (s *NodeFairScheduler) ClassPolicyFor(name string) *ClassPolicy {
	if byName := s.classes.Load(); byName != nil {
		return (*byName)[name]
	}
	return nil
}

func (s *NodeFairScheduler) heavyWindow(class string) time.Duration {
	if p := s.ClassPolicyFor(class); p != nil && p.HeavyPercent > 0 {
		return time.Duration(p.HeavyWindowSeconds) * time.Second
	}
	return 0
}

func (s *NodeFairScheduler) ensureStarted() {
	if s.started.CompareAndSwap(false, true) {
		go func() {
			t := time.NewTicker(fairRecomputeEvery)
			defer t.Stop()
			for range t.C {
				s.recompute()
			}
		}()
	}
}

// poolFairState 是调度器对一个池的记忆，只在 recompute 里读写。
type poolFairState struct {
	up, down poolDirectionState
}

type poolDirectionState struct {
	lastGranted, lastWaits uint64
	active                 bool
	idleTicks              int
	shared                 bool // 上一次写给整形器的是拥挤态
	usage                  usageWindow
}

// usageWindow 是最近 window 秒的滚动用量，按 heavyBuckets 段求和。
type usageWindow struct {
	window, span uint32
	ring         [heavyBuckets]uint64
	idx          int
	cur          uint64
	curTicks     uint32
}

func (w *usageWindow) add(delta uint64, window uint32) {
	if window == 0 {
		*w = usageWindow{}
		return
	}
	if w.window != window {
		*w = usageWindow{window: window, span: max(1, (window+heavyBuckets-1)/heavyBuckets)}
	}
	w.cur += delta
	w.curTicks++
	if w.curTicks >= w.span {
		w.ring[w.idx] = w.cur
		w.idx = (w.idx + 1) % heavyBuckets
		w.cur, w.curTicks = 0, 0
	}
}

// average 返回窗口内平均字节/秒（1 tick = 1 秒）。开始记账之前的时间按零算。
func (w *usageWindow) average() uint64 {
	if w.window == 0 {
		return 0
	}
	sum := w.cur
	for _, b := range w.ring {
		sum += b
	}
	return sum / uint64(heavyBuckets*w.span+w.curTicks)
}

// fairSlot 是一个活跃池在一个方向上的本 tick 注水工作区。
type fairSlot struct {
	key     string
	class   *ClassPolicy
	policy  TierPolicy
	shaper  *TierShaper
	delta   uint64
	blocked bool
	heavy   bool

	weight, ceiling, floor, want, alloc uint64
	pinned                              bool
}

type fillOutcome struct {
	truncated  bool
	rounds     int
	unresolved int
}

type poolRef struct {
	key   string
	entry *tierEntry
	class string
}

func snapshotPools() []poolRef {
	tierRegistry.Lock()
	defer tierRegistry.Unlock()
	out := make([]poolRef, 0, len(tierRegistry.m))
	for key, e := range tierRegistry.m {
		out = append(out, poolRef{key: key, entry: e, class: e.class})
	}
	return out
}

// recompute 是每 tick 的全部工作：采样 → 重度识别 → 拥塞判定 → 注水 → 写回整形器。
func (s *NodeFairScheduler) recompute() {
	root := s.rootCapBytePerSec.Load()
	pools := snapshotPools()

	s.mu.Lock()
	defer s.mu.Unlock()
	upOut, upCongested, upActive, upHeavy := s.direction(pools, root, true, &s.upCongestion)
	downOut, downCongested, downActive, downHeavy := s.direction(pools, root, false, &s.downCongestion)
	s.congested = upCongested || downCongested
	s.activeMembers.Store(uint32(max(upActive, downActive)))
	s.heavyMembers.Store(uint32(max(upHeavy, downHeavy)))
	s.noteFill(fillOutcome{
		truncated:  upOut.truncated || downOut.truncated,
		rounds:     max(upOut.rounds, downOut.rounds),
		unresolved: max(upOut.unresolved, downOut.unresolved),
	}, max(upActive, downActive))
}

func (s *NodeFairScheduler) direction(
	pools []poolRef, root uint64, upload bool, congestion *fairCongestionState,
) (fillOutcome, bool, int, int) {
	var used uint64
	slots := make([]*fairSlot, 0, len(pools))
	states := make([]*poolDirectionState, len(pools))
	heavy := 0
	for i, ref := range pools {
		shaper, st := ref.entry.down, &ref.entry.fair.down
		if upload {
			shaper, st = ref.entry.up, &ref.entry.fair.up
		}
		states[i] = st
		granted, waits, policy := shaper.sample()
		delta := granted - st.lastGranted
		blocked := waits != st.lastWaits
		st.lastGranted, st.lastWaits = granted, waits
		used += delta

		class := s.ClassPolicyFor(ref.class)
		var window uint32
		if class != nil && class.HeavyPercent > 0 {
			window = class.HeavyWindowSeconds
		}
		st.usage.add(delta, window)

		if st.active {
			if delta < fairActiveExitDeltaB {
				st.idleTicks++
				if st.idleTicks >= fairActiveExitTicks {
					st.active, st.idleTicks = false, 0
				}
			} else {
				st.idleTicks = 0
			}
		} else if delta > fairActiveEnterDeltaB || blocked {
			st.active, st.idleTicks = true, 0
		}
		if !st.active {
			continue
		}
		slot := &fairSlot{
			key: ref.key, class: class, policy: policy, shaper: shaper,
			delta: delta, blocked: blocked,
		}
		slot.heavy = window > 0 && policy.Standard > 0 &&
			st.usage.average()*100 >= policy.Standard*uint64(class.HeavyPercent)
		if slot.heavy {
			heavy++
		}
		slots = append(slots, slot)
	}
	if upload {
		s.usedUpload.Store(used)
	} else {
		s.usedDownload.Store(used)
	}

	congested := root > 0 && s.updateCongestion(congestion, used, root)
	if !congested {
		for i, ref := range pools {
			if states[i].shared {
				states[i].shared = false
				shaperOf(ref.entry, upload).setShare(false, 0)
			}
		}
		return fillOutcome{}, false, len(slots), heavy
	}
	outcome := s.fill(slots, root, upload)
	allocated := make(map[*TierShaper]uint64, len(slots))
	for _, slot := range slots {
		// 份额为 0 也要真的压住（1 B/s），0 在整形器里的意思是「封在标准」。
		allocated[slot.shaper] = max(slot.alloc, 1)
	}
	for i, ref := range pools {
		states[i].shared = true
		shaper := shaperOf(ref.entry, upload)
		shaper.setShare(true, allocated[shaper])
	}
	return outcome, true, len(slots), heavy
}

func shaperOf(e *tierEntry, upload bool) *TierShaper {
	if upload {
		return e.up
	}
	return e.down
}

// updateCongestion 维护拥塞滞回：越过 enter 进入；回落到 exit 以下并连续 exitTicks 个 tick 才退出。
func (s *NodeFairScheduler) updateCongestion(state *fairCongestionState, used, root uint64) bool {
	enter := uint64(s.congestionEnterPercent.Load())
	if enter == 0 {
		state.congested, state.belowExitTicks = true, 0
		return true
	}
	exit := uint64(s.congestionExitPercent.Load())
	if exit == 0 || exit > enter {
		exit = enter
	}
	exitTicks := max(int(s.congestionExitTicks.Load()), 1)
	util := used * 100 / root
	if !state.congested {
		if util >= enter {
			state.congested, state.belowExitTicks = true, 0
		}
		return state.congested
	}
	if util <= exit {
		state.belowExitTicks++
		if state.belowExitTicks >= exitTicks {
			state.congested, state.belowExitTicks = false, 0
		}
	} else {
		state.belowExitTicks = 0
	}
	return state.congested
}

type fairCongestionState struct {
	congested      bool
	belowExitTicks int
}

// fill 是拥挤时的注水本体，结果写进每个 slot 的 alloc。
func (s *NodeFairScheduler) fill(slots []*fairSlot, root uint64, upload bool) fillOutcome {
	// 顺序只影响注水截断时的近似，但同样的状态必须算出同样的结果。
	sort.Slice(slots, func(i, j int) bool { return slots[i].key < slots[j].key })
	for _, m := range slots {
		m.ceiling = root
		if std := m.policy.Standard; std > 0 && std < root {
			m.ceiling = std
		}
		m.weight = 1
		if m.class != nil && m.class.Weight > 0 {
			m.weight = uint64(m.class.Weight)
		}
		m.want = m.ceiling
		if !m.blocked {
			m.want = min(m.ceiling, m.delta+m.delta/fairSatisfiedHeadroomDiv)
		}
		m.floor, m.alloc, m.pinned = 0, 0, false
	}

	remaining := root - assignReserved(slots, root, upload)
	// 正常池地板，再重度池保底；每层给得起才给。
	for _, heavyLayer := range []bool{false, true} {
		var extra uint64
		for _, m := range slots {
			if m.heavy == heavyLayer {
				extra += layerFloor(m) - min(layerFloor(m), m.floor)
			}
		}
		if extra == 0 || extra > remaining {
			continue
		}
		for _, m := range slots {
			if m.heavy == heavyLayer {
				m.floor = max(m.floor, layerFloor(m))
			}
		}
		remaining -= extra
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
		// 谁也没被压住：节点其实不挤，别拿上一 tick 的实测把人钉死。
		for _, m := range slots {
			m.alloc = m.ceiling
		}
	} else {
		// 没排队的池（打网页、游戏、低于份额的推流）不按上一秒实测钉死，放到同层水位：
		// 否则一次开网页的突发要等到下一 tick 才拿得到带宽。这部分不预留，
		// 所以 Σ 份额可以暂时超过 root_cap，超出的只是这些池没用上的余量，最多持续一个 tick。
		raise := func(members []*fairSlot, level uint64) {
			for _, m := range members {
				if !m.blocked && level > 0 {
					m.alloc = max(m.alloc, min(m.ceiling, m.floor+level*m.weight))
				}
			}
		}
		raise(normal, normalLevel)
		raise(heavy, heavyLevel)
	}
	return fillOutcome{
		truncated:  normalOut.truncated || heavyOut.truncated,
		rounds:     max(normalOut.rounds, heavyOut.rounds),
		unresolved: normalOut.unresolved + heavyOut.unresolved,
	}
}

// layerFloor：正常池 = class 地板，重度池 = 持续速率；都不超过自己的需求。
func layerFloor(m *fairSlot) uint64 {
	f := m.policy.Sustained
	if !m.heavy {
		f = 0
		if m.class != nil {
			f = m.class.FloorBytePerSec
		}
	}
	return min(f, m.want)
}

// waterFill 在 members 之间按 weight 分 pool（每人已有 alloc=floor、want≥floor），返回剩余。
func waterFill(members []*fairSlot, pool uint64) (uint64, fillOutcome, bool, uint64) {
	out := fillOutcome{}
	for round := 0; ; round++ {
		out.rounds = round
		var totalWeight uint64
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
		var consumed uint64
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

func splitRemainder(members []*fairSlot, pool, totalWeight uint64) {
	for _, m := range members {
		if !m.pinned {
			m.alloc = m.floor + pool*m.weight/totalWeight
			m.pinned = true
		}
	}
}

// assignReserved 先发 class 聚合保底：同 class 的活跃池只分走各自用得上的部分，
// 没用上的当 tick 回流公共池。返回用掉多少。
func assignReserved(slots []*fairSlot, root uint64, upload bool) uint64 {
	groups := make(map[string][]*fairSlot)
	for _, m := range slots {
		if reservedOf(m.class, upload) > 0 {
			groups[m.class.Name] = append(groups[m.class.Name], m)
		}
	}
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	var used uint64
	for _, name := range names {
		members := groups[name]
		capacity := min(reservedOf(members[0].class, upload), root-used)
		used += aggregateFloor(members, capacity)
	}
	return used
}

func reservedOf(p *ClassPolicy, upload bool) uint64 {
	if p == nil {
		return 0
	}
	if upload {
		return p.UploadReservedBytePerSec
	}
	return p.DownloadReservedBytePerSec
}

// aggregateFloor 把 capacity 在 members 之间按需求做 max-min 分配，写进 floor。
func aggregateFloor(members []*fairSlot, capacity uint64) uint64 {
	pending := append([]*fairSlot(nil), members...)
	var used uint64
	for len(pending) > 0 && capacity > used {
		share := (capacity - used) / uint64(len(pending))
		if share == 0 {
			break
		}
		next := pending[:0]
		progressed := false
		for _, m := range pending {
			if need := m.want - m.floor; need <= share {
				m.floor += need
				used += need
				progressed = true
			} else {
				next = append(next, m)
			}
		}
		if !progressed {
			for _, m := range next {
				m.floor += share
				used += share
			}
			break
		}
		pending = next
	}
	return used
}

// FairShareStatus 是调度器运行态快照。
type FairShareStatus struct {
	RootCapBytePerSec      uint64 // 0 = 节点级调度没开
	Congested              bool   // 当前是否处于拥挤态
	ActiveMembers          uint32 // 最近一 tick 的活跃池数
	HeavyMembers           uint32 // 其中被识别为重度的池数
	UsedUploadBytePerSec   uint64 // 最近一 tick 的实测吞吐
	UsedDownloadBytePerSec uint64

	FillTruncated      bool
	FillUnresolved     uint32
	FillTruncatedTicks uint64
	FillTruncatedTotal uint64
	FillRounds         uint32
}

// Status 返回运行态快照，只在读拥塞态时短暂拿锁。
func (s *NodeFairScheduler) Status() FairShareStatus {
	st := FairShareStatus{
		RootCapBytePerSec:      s.rootCapBytePerSec.Load(),
		ActiveMembers:          s.activeMembers.Load(),
		HeavyMembers:           s.heavyMembers.Load(),
		UsedUploadBytePerSec:   s.usedUpload.Load(),
		UsedDownloadBytePerSec: s.usedDownload.Load(),
		FillTruncated:          s.fillTruncated.Load(),
		FillUnresolved:         s.fillUnresolved.Load(),
		FillTruncatedTicks:     s.fillTruncatedTicks.Load(),
		FillTruncatedTotal:     s.fillTruncatedTotal.Load(),
		FillRounds:             s.fillRounds.Load(),
	}
	s.mu.Lock()
	st.Congested = s.congested
	s.mu.Unlock()
	return st
}

// noteFill 记下注水结果；截断只在进入/退出时记日志，持续期间每 5 分钟复述一次。
func (s *NodeFairScheduler) noteFill(out fillOutcome, active int) {
	s.fillRounds.Store(uint32(out.rounds))
	if !out.truncated {
		s.fillUnresolved.Store(0)
		if s.fillTruncated.Swap(false) {
			held := s.fillTruncatedTicks.Swap(0)
			errors.LogInfo(context.Background(), "node fair share: 注水不再截断，本次持续了 ", held, " 秒")
		}
		return
	}
	s.fillUnresolved.Store(uint32(out.unresolved))
	s.fillTruncatedTotal.Add(1)
	held := s.fillTruncatedTicks.Add(1)
	if !s.fillTruncated.Swap(true) || held%fairTruncationRestateTicks == 0 {
		errors.LogWarning(context.Background(),
			"node fair share: 注水在 ", out.rounds, " 轮后截断，", out.unresolved, "/", active,
			" 个活跃池按权重一次分完；已持续 ", held, " 秒。总额仍不超过 root_cap。")
	}
}
