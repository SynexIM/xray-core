package stats

import (
	"github.com/xtls/xray-core/common/errors"
	featurestats "github.com/xtls/xray-core/features/stats"
	"sync"
)

const statisticsShards = 64

type counterShard struct {
	sync.RWMutex
	entries map[string]*Counter
}

func statisticsShard(name string) uint32 {
	hash := uint32(2166136261)
	for index := 0; index < len(name); index++ {
		hash ^= uint32(name[index])
		hash *= 16777619
	}
	return hash % statisticsShards
}
func (manager *Manager) counter(name string, create, exclusive bool) (featurestats.Counter, error) {
	if email, kind, ok := parseUserCounter(name); ok {
		return manager.userCounter(email, kind, create, exclusive)
	}
	shard := &manager.counterShards[statisticsShard(name)]
	shard.RLock()
	value := shard.entries[name]
	shard.RUnlock()
	if value != nil {
		if exclusive {
			return nil, errors.New("Counter ", name, " already registered.")
		}
		return value, nil
	}
	if !create {
		return nil, nil
	}
	shard.Lock()
	defer shard.Unlock()
	if value := shard.entries[name]; value != nil {
		if exclusive {
			return nil, errors.New("Counter ", name, " already registered.")
		}
		return value, nil
	}
	value = new(Counter)
	shard.entries[name] = value
	return value, nil
}
func (manager *Manager) RegisterCounter(name string) (featurestats.Counter, error) {
	return manager.counter(name, true, true)
}
func (manager *Manager) GetOrRegisterCounter(name string) (featurestats.Counter, error) {
	return manager.counter(name, true, false)
}
func (manager *Manager) GetCounter(name string) featurestats.Counter {
	value, _ := manager.counter(name, false, false)
	return value
}
func (manager *Manager) UnregisterCounter(name string) error {
	if email, kind, ok := parseUserCounter(name); ok {
		manager.removeUserCounter(email, kind)
		return nil
	}
	shard := &manager.counterShards[statisticsShard(name)]
	shard.Lock()
	delete(shard.entries, name)
	shard.Unlock()
	return nil
}
func (manager *Manager) VisitCounters(visitor func(string, featurestats.Counter) bool) {
	for index := range manager.counterShards {
		shard := &manager.counterShards[index]
		shard.RLock()
		for name, counter := range shard.entries {
			if !visitor(name, counter) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
	for index := range manager.userShards {
		shard := &manager.userShards[index]
		shard.RLock()
		for email, record := range shard.entries {
			for _, kind := range []uint32{counterUplink, counterDownlink, counterActive} {
				if record.enabled.Load()&kind != 0 && !visitor(userCounterName(email, kind), record.counter(kind)) {
					shard.RUnlock()
					return
				}
			}
		}
		shard.RUnlock()
	}
}
