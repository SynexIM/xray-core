package http

import (
	"sync"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/protocol"
)

var errUserMissingEmail = errors.New("user is missing an email (the per-user key)")
var errUserMissingUsername = errors.New("user is missing a username (the auth key)")

// accountPassword extracts the password from a runtime-added user's account. Both
// the http and socks account protos expose GetPassword(); a user with no such
// account (e.g. limits-only) authenticates with an empty password.
func accountPassword(u *protocol.MemoryUser) string {
	if u == nil || u.Account == nil {
		return ""
	}
	if a, ok := u.Account.(interface{ GetPassword() string }); ok {
		return a.GetPassword()
	}
	return ""
}

func accountUsername(u *protocol.MemoryUser) string {
	if u == nil || u.Account == nil {
		return ""
	}
	if a, ok := u.Account.(interface{ GetUsername() string }); ok {
		return a.GetUsername()
	}
	return ""
}

// UserStore 让 HTTP 与 SOCKS 共用身份表，凭据按用户名认证，管理按 email 定位。
type UserStore struct {
	mu       sync.RWMutex
	level    uint32
	users    map[string]*protocol.MemoryUser // username -> user
	password map[string]string               // username -> password
	emailKey map[string]string               // email -> username
}

// 两个协议共用启动账户的最小接口，避免循环导入。
type BootAccount interface {
	GetUsername() string
	GetPassword() string
	GetRuntime() *protocol.User
}

// 显式身份账户优先于旧的用户名密码映射。
func NewUserStore[T BootAccount](level uint32, accounts map[string]string, userAccounts []T) *UserStore {
	s := &UserStore{
		level:    level,
		users:    make(map[string]*protocol.MemoryUser),
		password: make(map[string]string),
		emailKey: make(map[string]string),
	}
	for username, password := range accounts {
		s.setRuntime(username, password, &protocol.MemoryUser{})
	}
	for _, ua := range userAccounts {
		if rt := ua.GetRuntime(); rt != nil {
			s.setRuntime(ua.GetUsername(), ua.GetPassword(), rt.RuntimeMemoryUser())
			continue
		}
		s.setRuntime(ua.GetUsername(), ua.GetPassword(), &protocol.MemoryUser{})
	}
	return s
}

// setRuntime installs a boot user whose email may differ from its username, so
// one logical client shares shaping and stats with its other inbounds.
func (s *UserStore) setRuntime(username, password string, u *protocol.MemoryUser) {
	if username == "" {
		return
	}
	u.Level = s.level
	if u.Email == "" {
		u.Email = username
	}
	s.users[username] = u
	s.password[username] = password
	s.emailKey[u.Email] = username
}

// Empty reports whether the store has no users (anonymous inbound).
func (s *UserStore) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users) == 0
}

// 认证只在读锁内读取，凭据热改不产生空表窗口。
func (s *UserStore) Authenticate(username, password string) (*protocol.MemoryUser, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, found := s.password[username]
	if !found || stored != password {
		return nil, false
	}
	return s.users[username], true
}

func (s *UserStore) Add(u *protocol.MemoryUser) error {
	if u == nil || u.Email == "" {
		return errUserMissingEmail
	}
	username := accountUsername(u)
	if username == "" {
		username = u.Email
	}
	if username == "" {
		return errUserMissingUsername
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldUsername := s.emailKey[u.Email]; oldUsername != "" && oldUsername != username {
		delete(s.users, oldUsername)
		delete(s.password, oldUsername)
	}
	s.users[username] = u
	s.password[username] = accountPassword(u)
	s.emailKey[u.Email] = username
	return nil
}

// 凭据替换保留同一身份的会话生命周期，不中断已建立连接。
func (s *UserStore) Update(u *protocol.MemoryUser) error {
	if u == nil || u.Email == "" {
		return errUserMissingEmail
	}
	username := accountUsername(u)
	if username == "" {
		username = u.Email
	}
	if username == "" {
		return errUserMissingUsername
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	oldUsername := s.emailKey[u.Email]
	if oldUsername == "" {
		return errors.New("user to update does not exist: ", u.Email)
	}
	if old := s.users[oldUsername]; old != nil {
		u.ShareRuntimeSessions(old)
	}
	if oldUsername != username {
		delete(s.users, oldUsername)
		delete(s.password, oldUsername)
	}
	s.users[username] = u
	s.password[username] = accountPassword(u)
	s.emailKey[u.Email] = username
	return nil
}

// 撤销身份同时断开现有会话。
func (s *UserStore) Remove(email string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	username := s.emailKey[email]
	if username == "" {
		username = email
	}
	if u := s.users[username]; u != nil {
		u.RevokeRuntimeSessions()
	}
	delete(s.users, username)
	delete(s.password, username)
	delete(s.emailKey, email)
}

// Get returns the shared user by email, or nil.
func (s *UserStore) Get(email string) *protocol.MemoryUser {
	s.mu.RLock()
	defer s.mu.RUnlock()
	username := s.emailKey[email]
	if username == "" {
		username = email
	}
	return s.users[username]
}

// GetAll returns a snapshot of every user.
func (s *UserStore) GetAll() []*protocol.MemoryUser {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*protocol.MemoryUser, 0, len(s.users))
	for _, u := range s.users {
		out = append(out, u)
	}
	return out
}
