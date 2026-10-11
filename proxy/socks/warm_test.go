package socks

import (
	"errors"
	gonet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/transport/internet/stat"
)

// The pool only fills after demand, never above warmMax, backs off after a
// failed refill, and stops for warmBackoff after repeated stale connections.
func TestWarmPoolFollowsDemand(t *testing.T) {
	w := &warmPool{}
	now := time.Now()
	if w.take(now) != nil || w.target(now) != 1 {
		t.Fatalf("one request should ask for one warm connection, target=%d", w.target(now))
	}
	for range 10 {
		w.take(now)
	}
	if got := w.target(now); got != warmMax {
		t.Fatalf("target=%d, want cap %d", got, warmMax)
	}

	var mu sync.Mutex
	opened := 0
	open := func() (stat.Connection, error) {
		mu.Lock()
		opened++
		mu.Unlock()
		a, _ := gonet.Pipe()
		return a, nil
	}
	settled := func(p *warmPool) {
		for {
			p.mu.Lock()
			f := p.filling
			p.mu.Unlock()
			if f == 0 {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	w.refill(open)
	settled(w)
	w.refill(open) // already full: no more dials
	settled(w)
	mu.Lock()
	if opened != warmMax || len(w.idle) != warmMax {
		t.Fatalf("opened=%d idle=%d", opened, len(w.idle))
	}
	mu.Unlock()

	w2 := &warmPool{}
	w2.take(now)
	w2.refill(func() (stat.Connection, error) { return nil, errors.New("down") })
	settled(w2)
	w2.mu.Lock()
	if w2.target(time.Now()) != 0 {
		t.Fatal("a failed refill must back off")
	}
	w2.mu.Unlock()

	for range warmMaxMisses {
		w.miss(now)
	}
	if w.target(now) != 0 || len(w.idle) != 0 {
		t.Fatal("repeated stale connections must disable the pool")
	}
}
