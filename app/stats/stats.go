package stats

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/stats"
)

// Manager is an implementation of stats.Manager.
type Manager struct {
	access        sync.RWMutex
	counterShards [statisticsShards]counterShard
	userShards    [statisticsShards]userShard
	changedShards [statisticsShards]changedShard
	onlineMaps    map[string]*OnlineMap
	channels      map[string]*Channel
	running       bool
}

// NewManager creates an instance of Statistics Manager.
func NewManager(ctx context.Context, config *Config) (*Manager, error) {
	m := &Manager{
		onlineMaps: make(map[string]*OnlineMap),
		channels:   make(map[string]*Channel),
	}

	for index := range m.counterShards {
		m.counterShards[index].entries = make(map[string]*Counter)
		m.userShards[index].entries = make(map[string]*userRecord)
		m.changedShards[index].entries = make(map[string]*userRecord)
	}
	return m, nil
}

// Type implements common.HasType.
func (*Manager) Type() interface{} {
	return stats.ManagerType()
}

// RegisterOnlineMap implements stats.Manager.
func (m *Manager) RegisterOnlineMap(name string) (stats.OnlineMap, error) {
	if email, ok := parseUserOnline(name); ok {
		return m.userOnline(email, true, true)
	}
	m.access.Lock()
	defer m.access.Unlock()

	if _, found := m.onlineMaps[name]; found {
		return nil, errors.New("OnlineMap ", name, " already registered.")
	}
	errors.LogDebug(context.Background(), "create new OnlineMap ", name)
	om := NewOnlineMap()
	m.onlineMaps[name] = om
	return om, nil
}

// GetOrRegisterOnlineMap implements stats.Manager.
func (m *Manager) GetOrRegisterOnlineMap(name string) (stats.OnlineMap, error) {
	if email, ok := parseUserOnline(name); ok {
		return m.userOnline(email, true, false)
	}
	m.access.Lock()
	defer m.access.Unlock()

	if om, found := m.onlineMaps[name]; found {
		return om, nil
	}
	errors.LogDebug(context.Background(), "create new OnlineMap ", name)
	om := NewOnlineMap()
	m.onlineMaps[name] = om
	return om, nil
}

// UnregisterOnlineMap implements stats.Manager.
func (m *Manager) UnregisterOnlineMap(name string) error {
	if email, ok := parseUserOnline(name); ok {
		m.removeUserOnline(email)
		return nil
	}
	m.access.Lock()
	defer m.access.Unlock()

	if _, found := m.onlineMaps[name]; found {
		errors.LogDebug(context.Background(), "remove OnlineMap ", name)
		delete(m.onlineMaps, name)
	}
	return nil
}

// GetOnlineMap implements stats.Manager.
func (m *Manager) GetOnlineMap(name string) stats.OnlineMap {
	if email, ok := parseUserOnline(name); ok {
		value, _ := m.userOnline(email, false, false)
		return value
	}
	m.access.RLock()
	defer m.access.RUnlock()

	if om, found := m.onlineMaps[name]; found {
		return om
	}
	return nil
}

// VisitOnlineMaps calls visitor function on all managed online maps.
// The visitor runs under a read lock; it must not call RegisterOnlineMap or UnregisterOnlineMap (would deadlock).
func (m *Manager) VisitOnlineMaps(visitor func(string, stats.OnlineMap) bool) {
	for index := range m.userShards {
		shard := &m.userShards[index]
		shard.RLock()
		for email, record := range shard.entries {
			if online := record.online.Load(); online != nil {
				if !visitor(stats.UserOnlineMapName(email), online) {
					shard.RUnlock()
					return
				}
			}
		}
		shard.RUnlock()
	}

	m.access.RLock()
	defer m.access.RUnlock()
	for name, om := range m.onlineMaps {
		if !visitor(name, om) {
			break
		}
	}
}

// RegisterChannel implements stats.Manager.
func (m *Manager) RegisterChannel(name string) (stats.Channel, error) {
	m.access.Lock()
	defer m.access.Unlock()

	if _, found := m.channels[name]; found {
		return nil, errors.New("Channel ", name, " already registered.")
	}
	errors.LogDebug(context.Background(), "create new channel ", name)
	c := NewChannel(&ChannelConfig{BufferSize: 64, Blocking: false})
	m.channels[name] = c
	if m.running {
		return c, c.Start()
	}
	return c, nil
}

// GetOrRegisterChannel implements stats.Manager.
func (m *Manager) GetOrRegisterChannel(name string) (stats.Channel, error) {
	m.access.Lock()
	defer m.access.Unlock()

	if c, found := m.channels[name]; found {
		return c, nil
	}
	errors.LogDebug(context.Background(), "create new channel ", name)
	c := NewChannel(&ChannelConfig{BufferSize: 64, Blocking: false})
	if m.running {
		// Start before publishing so no goroutine can observe an unstarted channel.
		if err := c.Start(); err != nil {
			return nil, err
		}
	}
	m.channels[name] = c
	return c, nil
}

// UnregisterChannel implements stats.Manager.
func (m *Manager) UnregisterChannel(name string) error {
	m.access.Lock()
	defer m.access.Unlock()

	if c, found := m.channels[name]; found {
		errors.LogDebug(context.Background(), "remove channel ", name)
		delete(m.channels, name)
		return c.Close()
	}
	return nil
}

// GetChannel implements stats.Manager.
func (m *Manager) GetChannel(name string) stats.Channel {
	m.access.RLock()
	defer m.access.RUnlock()

	if c, found := m.channels[name]; found {
		return c
	}
	return nil
}

// GetAllOnlineUsers implements stats.Manager.
func (m *Manager) GetAllOnlineUsers() []string {
	var usersOnline []string
	m.VisitOnlineMaps(func(name string, online stats.OnlineMap) bool {
		if online.Count() > 0 {
			usersOnline = append(usersOnline, name)
		}
		return true
	})
	return usersOnline
}

// Start implements common.Runnable.
func (m *Manager) Start() error {
	m.access.Lock()
	defer m.access.Unlock()
	m.running = true
	errs := []error{}
	for _, channel := range m.channels {
		if err := channel.Start(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Combine(errs...)
	}
	return nil
}

// Close implement common.Closable.
func (m *Manager) Close() error {
	m.access.Lock()
	defer m.access.Unlock()
	m.running = false
	for name := range m.onlineMaps {
		errors.LogDebug(context.Background(), "remove OnlineMap ", name)
		delete(m.onlineMaps, name)
	}
	errs := []error{}
	for name, channel := range m.channels {
		errors.LogDebug(context.Background(), "remove channel ", name)
		delete(m.channels, name)
		if err := channel.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errors.Combine(errs...)
	}
	return nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return NewManager(ctx, config.(*Config))
	}))
}
