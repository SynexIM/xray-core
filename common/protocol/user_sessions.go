package protocol

import (
	"sync"
)

type userSessions struct {
	sync.Mutex
	revoked bool
}

var emailSessions = struct {
	sync.Mutex
	next    uint64
	entries map[string]map[uint64]func()
}{entries: map[string]map[uint64]func(){}}

// 会话状态由身份拥有，避免每个已认证身份进入全局弱表和 GC cleanup 链。
func (u *MemoryUser) sessions() *userSessions {
	if state := u.runtimeSessions.Load(); state != nil {
		return state
	}
	state := new(userSessions)
	if u.runtimeSessions.CompareAndSwap(nil, state) {
		return state
	}
	return u.runtimeSessions.Load()
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
		u.shareStats(previous)
		u.runtimeSessions.Store(previous.sessions())
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
	u.ReleaseStats()
}
