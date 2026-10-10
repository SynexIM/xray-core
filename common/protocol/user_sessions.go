package protocol

import (
	"runtime"
	"sync"
	"weak"
)

type userSessions struct {
	sync.Mutex
	revoked bool
}

var identities sync.Map
var emailSessions = struct {
	sync.Mutex
	next    uint64
	entries map[string]map[uint64]func()
}{entries: map[string]map[uint64]func(){}}

// 弱身份键用于拦住删除与认证的竞态，不把已删除的用户留在进程里。
func (u *MemoryUser) sessions() *userSessions {
	key := weak.Make(u)
	if value, ok := identities.Load(key); ok {
		return value.(*userSessions)
	}
	actual, loaded := identities.LoadOrStore(key, &userSessions{})
	if !loaded {
		runtime.AddCleanup(u, func(k weak.Pointer[MemoryUser]) { identities.Delete(k) }, key)
	}
	return actual.(*userSessions)
}
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
	emailSessions.Lock()
	emailSessions.next++
	id := emailSessions.next
	email := u.Email
	if emailSessions.entries[email] == nil {
		emailSessions.entries[email] = map[uint64]func(){}
	}
	emailSessions.entries[email][id] = stop
	emailSessions.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			emailSessions.Lock()
			delete(emailSessions.entries[email], id)
			if len(emailSessions.entries[email]) == 0 {
				delete(emailSessions.entries, email)
			}
			emailSessions.Unlock()
		})
	}, true
}
func (u *MemoryUser) ShareRuntimeSessions(previous *MemoryUser) {
	if u != nil && previous != nil && u != previous {
		key := weak.Make(u)
		identities.Store(key, previous.sessions())
		runtime.AddCleanup(u, func(k weak.Pointer[MemoryUser]) { identities.Delete(k) }, key)
	}
}

// KillUserConnections 只断现有会话，身份仍能建立新连接。
func KillUserConnections(emails []string) {
	var callbacks []func()
	emailSessions.Lock()
	for _, email := range emails {
		for _, stop := range emailSessions.entries[email] {
			callbacks = append(callbacks, stop)
		}
		delete(emailSessions.entries, email)
	}
	emailSessions.Unlock()
	for _, stop := range callbacks {
		stop()
	}
}
func (u *MemoryUser) RevokeRuntimeSessions() {
	if u == nil {
		return
	}
	state := u.sessions()
	state.Lock()
	state.revoked = true
	state.Unlock()
	KillUserConnections([]string{u.Email})
}
