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

// UserStore is an in-memory, concurrency-safe username->user table shared by the
// http and (via embedding) mixed/socks inbounds. It is the per-user limit carrier
// those static proxies historically lacked: authentication returns a single
// stable *protocol.MemoryUser per username, so the dispatcher's bandwidth, fair-
// share and connection-cap enforcement (all keyed off the *MemoryUser instance)
// apply to mixed exactly as they do to vless. It also backs proxy.UserManager so
// users can be added/removed at runtime without rebuilding the inbound.
type UserStore struct {
	mu       sync.RWMutex
	level    uint32
	users    map[string]*protocol.MemoryUser // username -> user
	password map[string]string               // username -> password
	emailKey map[string]string               // email -> username
}

// LimitedAccount is the shape of a boot-time user account carrying per-user
// limits. Both http.UserAccount and socks.UserAccount satisfy it, so NewUserStore
// can build a shared store for the mixed inbound without importing socks (which
// would create a cycle).
type LimitedAccount interface {
	GetUsername() string
	GetPassword() string
	GetBandwidthBps() uint64
	GetConnLimit() uint32
	GetCommittedBps() uint64
	GetCommittedBurstBytes() uint64
}

// NewUserStore builds a store from boot accounts. level is the inbound user
// level applied to every user. accounts is the legacy username->password map
// (no limits); userAccounts carries per-user limits and takes precedence.
func NewUserStore[T LimitedAccount](level uint32, accounts map[string]string, userAccounts []T) *UserStore {
	s := &UserStore{
		level:    level,
		users:    make(map[string]*protocol.MemoryUser),
		password: make(map[string]string),
		emailKey: make(map[string]string),
	}
	for username, password := range accounts {
		s.set(username, password, limits{})
	}
	for _, ua := range userAccounts {
		s.set(ua.GetUsername(), ua.GetPassword(), limits{
			bandwidthBps:        ua.GetBandwidthBps(),
			connLimit:           ua.GetConnLimit(),
			committedBps:        ua.GetCommittedBps(),
			committedBurstBytes: ua.GetCommittedBurstBytes(),
		})
	}
	return s
}

// limits 把一个用户的四个限速字段捆成一个参数，免得 set 变成一串裸数字
// ——那种签名下多加一个字段就很容易在某个调用点漏掉，而漏掉是静默的。
type limits struct {
	bandwidthBps        uint64
	connLimit           uint32
	committedBps        uint64
	committedBurstBytes uint64
}

func (s *UserStore) set(username, password string, l limits) {
	if username == "" {
		return
	}
	s.users[username] = &protocol.MemoryUser{
		Email:               username,
		Level:               s.level,
		BandwidthBps:        l.bandwidthBps,
		ConnLimit:           l.connLimit,
		CommittedBps:        l.committedBps,
		CommittedBurstBytes: l.committedBurstBytes,
	}
	s.password[username] = password
	s.emailKey[username] = username
}

// Empty reports whether the store has no users (anonymous inbound).
func (s *UserStore) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users) == 0
}

// Authenticate verifies username/password and returns the shared *MemoryUser for
// that username. The same pointer is returned for every connection of a user, so
// fair-share bandwidth and connection counting stay per-user, not per-connection.
func (s *UserStore) Authenticate(username, password string) (*protocol.MemoryUser, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	stored, found := s.password[username]
	if !found || stored != password {
		return nil, false
	}
	return s.users[username], true
}

// Add registers/replaces a user (runtime AddUser). The MemoryUser already
// carries top-level limits via ToMemoryUser.
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

// Update replaces an existing user (keyed by email) in place, applying new
// credentials and per-user limits. It is the UpdateUser path: a single atomic
// swap that avoids the remove+add round trip and the transient window in which
// the email has no user. The old *MemoryUser's runtime limiter and connection
// counter are reset because both maps are keyed by the user pointer and Update
// installs a fresh pointer; without the reset the old map entries would leak.
// The new MemoryUser carries the new bandwidth_bps / conn_limit, so the
// dispatcher's per-user enforcement picks up the new caps for subsequent
// connections. Returns an error if the email is not already present so callers
// do not silently turn an update into a create.
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
		old.ResetRuntimeLimiter()
		old.ResetRuntimeConnections()
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

// Remove drops a user and releases its runtime limiter + connection counter so a
// re-added user with the same name does not inherit stale state.
func (s *UserStore) Remove(email string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	username := s.emailKey[email]
	if username == "" {
		username = email
	}
	if u := s.users[username]; u != nil {
		u.ResetRuntimeLimiter()
		u.ResetRuntimeConnections()
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
