package limit

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// 小流可以借一个量子，满载流按字节虚拟标签轮转；等待只占各自 channel。
const (
	shaperQuantum        = 25 * time.Millisecond
	shaperSlice          = 5 * time.Millisecond
	shaperMinQuantumByte = 3000
	shaperMinSliceByte   = 1500
)

var errShaperClosed = errors.New("limit: 整形器已关闭")

type ladderBucket struct{ rate, depth, tokens float64 }

// 一个规则一个方向的所有阶梯桶共用队列，避免串联桶各自 FIFO 带来的队头阻塞。
type Shaper struct {
	mu             sync.Mutex
	buckets        []ladderBucket
	rate, tokens   float64
	last           time.Time
	vtime, seq     uint64
	queue          waitQueue
	timer          *time.Timer
	armed, closed  bool
	active         map[*Flow]struct{}
	lastSweep      time.Time
	congested      bool
	share          float64
	granted, waits uint64
}

func newShaper() *Shaper {
	return &Shaper{last: time.Now(), active: map[*Flow]struct{}{}}
}
func (s *Shaper) Configure(l *Ladder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	old := s.buckets
	var next []ladderBucket
	if l != nil {
		// 相邻到达曲线在累计时长 T_i 相交：b_i=b_(i-1)+(r_(i-1)-r_i)*T_i。
		var elapsed, depth float64
		for i, step := range l.Steps {
			rate := float64(step.RateBps) / 8
			if i == 0 {
				depth = quantumBytesAt(rate)
			} else {
				elapsed += float64(l.Steps[i-1].ForMs) / 1000
				depth += (float64(l.Steps[i-1].RateBps)/8 - rate) * elapsed
			}
			tokens := depth
			if i < len(old) {
				tokens = min(tokens, old[i].tokens)
			}
			next = append(next, ladderBucket{rate: rate, depth: depth, tokens: tokens})
		}
	}
	s.buckets = next
	s.updateRate()
	s.rearmLocked()
	s.dispatchLocked()
}
func (s *Shaper) rearmLocked() {
	if s.timer != nil {
		s.timer.Stop()
	}
	s.armed = false
}
func (s *Shaper) setShare(congested bool, share float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	s.congested = congested
	s.share = float64(share)
	s.updateRate()
	s.rearmLocked()
	s.dispatchLocked()
}
func (s *Shaper) sample() (uint64, uint64, float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	rate, _ := s.ladderRate()
	return s.granted, s.waits, rate
}

type DirectionStatus struct {
	RateBps        uint64
	Step           uint32
	RemainingBytes []uint64
	CapacityBytes  []uint64
}

func (s *Shaper) Snapshot() DirectionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	_, step := s.ladderRate()
	currentStep := uint32(0)
	if len(s.buckets) > 0 {
		currentStep = uint32(step + 1)
	}
	result := DirectionStatus{RateBps: statusUint(s.rate * 8), Step: currentStep}
	for _, b := range s.buckets {
		result.RemainingBytes = append(result.RemainingBytes, statusUint(b.tokens))
		result.CapacityBytes = append(result.CapacityBytes, statusUint(b.depth))
	}
	return result
}
func statusUint(value float64) uint64 {
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxUint64) {
		return math.MaxUint64
	}
	return uint64(value)
}
func (s *Shaper) currentRate() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	if s.rate == 0 && !s.congested {
		return math.Inf(1)
	}
	return s.rate
}

// 回满前不得丢弃桶，否则断开重连会获得额外额度。
func (s *Shaper) idleTTL() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advance(time.Now())
	var seconds float64
	for _, b := range s.buckets {
		seconds = max(seconds, (b.depth-b.tokens)/b.rate)
	}
	if seconds >= float64(math.MaxInt64)/float64(time.Second) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}
func quantumBytesAt(rate float64) float64 {
	return max(rate*shaperQuantum.Seconds(), shaperMinQuantumByte)
}
func (s *Shaper) quantumBytes() float64 { return quantumBytesAt(s.rate) }
func (s *Shaper) sliceBytes() int {
	return int(min(float64(math.MaxInt), max(s.rate*shaperSlice.Seconds(), shaperMinSliceByte)))
}
func (s *Shaper) fairRate() float64 { return s.rate / float64(max(len(s.active), 1)) }
func (s *Shaper) meter(f *Flow, now time.Time) {
	if dt := now.Sub(f.at).Seconds(); dt > 0 {
		f.used = max(0, f.used-s.fairRate()*dt)
		f.at = now
	}
}
func (s *Shaper) sweep(now time.Time) {
	if now.Sub(s.lastSweep) < time.Millisecond {
		return
	}
	s.lastSweep = now
	for f := range s.active {
		s.meter(f, now)
		if f.used == 0 && !f.waiting {
			delete(s.active, f)
		}
	}
}
func (s *Shaper) sparse(f *Flow, n int) bool {
	return f.used+float64(n) <= max(s.fairRate()*shaperQuantum.Seconds(), shaperMinQuantumByte)
}
func (s *Shaper) advance(now time.Time) {
	dt := now.Sub(s.last).Seconds()
	if dt <= 0 {
		return
	}
	s.last = now
	for i := range s.buckets {
		b := &s.buckets[i]
		b.tokens = min(b.depth, b.tokens+b.rate*dt)
	}
	if s.rate > 0 {
		s.tokens = min(s.quantumBytes(), s.tokens+s.rate*dt)
	}
	s.updateRate()
}
func (s *Shaper) ladderRate() (float64, int) {
	if len(s.buckets) == 0 {
		return 0, 0
	}
	rate := s.buckets[0].rate
	step := 0
	for i := 1; i < len(s.buckets); i++ {
		b := s.buckets[i]
		available := b.rate + max(0, b.tokens)/shaperQuantum.Seconds()
		if available < rate {
			rate = available
			step = i
		}
	}
	return rate, step
}
func (s *Shaper) updateRate() {
	rate, _ := s.ladderRate()
	if s.congested && (rate == 0 || s.share < rate) {
		rate = s.share
	}
	if rate == s.rate {
		return
	}
	if s.rate == 0 && rate > 0 {
		s.tokens = quantumBytesAt(rate)
	}
	s.rate = rate
	s.tokens = min(s.tokens, s.quantumBytes())
}
func (s *Shaper) grantLocked(f *Flow, n int) {
	s.tokens -= float64(n)
	s.granted += uint64(n)
	for i := range s.buckets {
		s.buckets[i].tokens -= float64(n)
	}
	f.used += float64(n)
	s.active[f] = struct{}{}
	s.updateRate()
}
func (s *Shaper) ready(w *waiter) bool {
	if s.rate <= 0 {
		return false
	}
	if w.sparse {
		if s.tokens-float64(w.n) < -s.quantumBytes() {
			return false
		}
		for _, b := range s.buckets {
			if b.tokens-float64(w.n) < -quantumBytesAt(b.rate) {
				return false
			}
		}
		return true
	}
	if s.tokens <= 0 {
		return false
	}
	for _, b := range s.buckets {
		if b.tokens <= 0 {
			return false
		}
	}
	return true
}
func (s *Shaper) dispatchLocked() {
	for s.queue.Len() > 0 && ((s.rate == 0 && !s.congested) || (s.rate > 0 && s.ready(s.queue[0]))) {
		w := heap.Pop(&s.queue).(*waiter)
		w.flow.waiting = false
		s.grantLocked(w.flow, w.n)
		if !w.sparse {
			s.vtime = max(s.vtime, w.start)
		}
		close(w.ready)
	}
	if s.queue.Len() == 0 || s.armed || s.closed || (s.rate == 0 && s.congested) {
		return
	}
	need := 1.0
	if head := s.queue[0]; head.sparse {
		need = float64(head.n) - s.quantumBytes()
	}
	delay := time.Duration(max(need-s.tokens, 1) / s.rate * float64(time.Second))
	for _, b := range s.buckets {
		need := 1.0
		if head := s.queue[0]; head.sparse {
			need = float64(head.n) - quantumBytesAt(b.rate)
		}
		wait := time.Duration(max(need-b.tokens, 1) / b.rate * float64(time.Second))
		delay = max(delay, wait)
	}
	s.armed = true
	if s.timer == nil {
		s.timer = time.AfterFunc(delay, s.onTimer)
	} else {
		s.timer.Reset(delay)
	}
}
func (s *Shaper) onTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = false
	if s.closed {
		return
	}
	s.advance(time.Now())
	s.dispatchLocked()
}
func (s *Shaper) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.rearmLocked()
	for s.queue.Len() > 0 {
		w := heap.Pop(&s.queue).(*waiter)
		w.flow.waiting = false
		w.err = errShaperClosed
		close(w.ready)
	}
	clear(s.active)
}
func (f *Flow) release() {
	if f == nil {
		return
	}
	f.s.mu.Lock()
	delete(f.s.active, f)
	f.s.mu.Unlock()
}

// NewFlow 给一条连接的一个方向建公平排队身份。
func (s *Shaper) NewFlow() *Flow {
	if s == nil {
		return nil
	}
	return &Flow{s: s, at: time.Now()}
}

// Flow 是一条连接在整形器里的身份：finish 是满载时的虚拟结束标签，used 是按公平份额
// 漏掉的近期用量（判稀疏用）。一条连接一个方向同一时刻只有一个 Wait。
type Flow struct {
	s       *Shaper
	finish  uint64
	used    float64
	at      time.Time
	waiting bool
}

// Wait 阻塞到 n 字节全部被放行。稀疏连接直接放行（桶欠账），满载连接按 5ms 的片排队轮转。
func (f *Flow) Wait(ctx context.Context, n int) error {
	s := f.s
	for n > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return errShaperClosed
		}
		now := time.Now()
		s.advance(now)
		if s.rate == 0 && !s.congested {
			// 不限速也记账：调度器要靠它判断节点忙不忙。
			s.granted += uint64(n)
			s.mu.Unlock()
			return nil
		}
		s.sweep(now)
		s.meter(f, now)
		chunk := min(n, s.sliceBytes())
		sparse := s.sparse(f, chunk)
		free := s.queue.Len() == 0 && s.ready(&waiter{n: chunk})
		if free || (sparse && !s.sparseQueued() && s.ready(&waiter{n: chunk, sparse: true})) {
			s.grantLocked(f, chunk)
			s.mu.Unlock()
			n -= chunk
			continue
		}
		s.seq++
		s.waits++
		w := &waiter{flow: f, sparse: sparse, seq: s.seq, n: chunk, ready: make(chan struct{})}
		if !sparse {
			w.start = max(s.vtime, f.finish)
			f.finish = w.start + uint64(chunk)
		}
		heap.Push(&s.queue, w)
		f.waiting = true
		s.active[f] = struct{}{}
		s.dispatchLocked()
		s.mu.Unlock()
		select {
		case <-w.ready:
			if w.err != nil {
				return w.err
			}
			n -= chunk
		case <-ctx.Done():
			s.mu.Lock()
			if w.index >= 0 {
				heap.Remove(&s.queue, w.index)
				f.waiting = false
				delete(s.active, f)
				s.dispatchLocked()
			}
			s.mu.Unlock()
			return ctx.Err()
		}
	}
	return nil
}

// sparseQueued：已经有稀疏等待者（稀疏合计超了桶速率），新来的稀疏片按先来后到排在它后面。
func (s *Shaper) sparseQueued() bool {
	return s.queue.Len() > 0 && s.queue[0].sparse
}

type waiter struct {
	flow       *Flow
	sparse     bool
	start, seq uint64
	n          int
	index      int
	ready      chan struct{}
	err        error
}

type waitQueue []*waiter

func (q waitQueue) Len() int { return len(q) }
func (q waitQueue) Less(i, j int) bool {
	if q[i].sparse != q[j].sparse {
		return q[i].sparse
	}
	if q[i].start != q[j].start {
		return q[i].start < q[j].start
	}
	return q[i].seq < q[j].seq
}

func (q waitQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index, q[j].index = i, j
}

func (q *waitQueue) Push(x any) {
	w := x.(*waiter)
	w.index = len(*q)
	*q = append(*q, w)
}

func (q *waitQueue) Pop() any {
	old := *q
	w := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	w.index = -1
	return w
}
