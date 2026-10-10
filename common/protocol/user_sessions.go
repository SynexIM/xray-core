package protocol

import "sync"

type userSessions struct {
	sync.Mutex
	revoked bool
	next    uint64
	stop    map[uint64]func()
}

func (u *MemoryUser) sessions() *userSessions {
	current := u.runtimeSessions.Load()
	if current != nil {
		return current
	}
	created := &userSessions{stop: map[uint64]func(){}}
	if u.runtimeSessions.CompareAndSwap(nil, created) {
		return created
	}
	return u.runtimeSessions.Load()
}

// Authentication may race removal. A retired identity cannot register another
// stream; hot updates share this lifetime with the replacement user object.
func (u *MemoryUser) TrackRuntimeSession(stop func()) (func(), bool) {
	if u == nil || u.Email == "" {
		return func() {}, true
	}
	state := u.sessions()
	state.Lock()
	defer state.Unlock()
	if state.revoked {
		return func() {}, false
	}
	state.next++
	id := state.next
	state.stop[id] = stop
	return func() { state.Lock(); delete(state.stop, id); state.Unlock() }, true
}
func (u *MemoryUser) ShareRuntimeSessions(previous *MemoryUser) {
	if u != nil && previous != nil {
		u.runtimeSessions.Store(previous.sessions())
	}
}
func (u *MemoryUser) RevokeRuntimeSessions() {
	if u == nil {
		return
	}
	state := u.sessions()
	state.Lock()
	state.revoked = true
	callbacks := state.stop
	state.stop = map[uint64]func(){}
	state.Unlock()
	for _, stop := range callbacks {
		stop()
	}
}
