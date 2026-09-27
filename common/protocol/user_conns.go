package protocol

import (
	"sync"
	"sync/atomic"
)

// userConnCounters tracks the number of concurrent connections per user. Keyed
// by *MemoryUser (the same key fair-share bandwidth uses) so that a user's cap
// follows the user instance across every protocol, and so RemoveUser can drop
// the counter precisely without email-collision risk.
var userConnCounters sync.Map // *MemoryUser -> *int64

// AcquireRuntimeConnection reserves one connection slot for the user. It returns
// a release func and ok=true when allowed; release must be called exactly once
// when the connection ends. When the user has no ConnLimit it is a no-op slot
// that always succeeds. When the cap is exceeded ok=false and no slot is held.
func (u *MemoryUser) AcquireRuntimeConnection() (release func(), ok bool) {
	if u == nil || u.ConnLimit == 0 {
		return func() {}, true
	}
	// Fast path: a plain Load hit avoids allocating a counter on every connection
	// (LoadOrStore eagerly evaluates its value argument).
	raw, ok := userConnCounters.Load(u)
	if !ok {
		raw, _ = userConnCounters.LoadOrStore(u, new(int64))
	}
	counter := raw.(*int64)
	if atomic.AddInt64(counter, 1) > int64(u.ConnLimit) {
		atomic.AddInt64(counter, -1)
		return func() {}, false
	}
	var once sync.Once
	return func() {
		once.Do(func() { atomic.AddInt64(counter, -1) })
	}, true
}

// ResetRuntimeConnections drops the user's connection counter. Call it from the
// protocol-agnostic RemoveUser path so a re-added user with the same email does
// not inherit a stale count from the previous *MemoryUser instance.
func (u *MemoryUser) ResetRuntimeConnections() {
	if u == nil {
		return
	}
	userConnCounters.Delete(u)
}
