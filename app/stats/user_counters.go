package stats

import (
	"github.com/xtls/xray-core/common/errors"
	featurestats "github.com/xtls/xray-core/features/stats"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	counterUplink uint32 = 1 << iota
	counterDownlink
	counterActive
	counterOnline
)

type userShard struct {
	sync.RWMutex
	entries map[string]*userRecord
}
type changedShard struct {
	sync.Mutex
	entries map[string]*userRecord
}
type userRecord struct {
	siteAccess               sync.Mutex
	sites                    map[string]struct{}
	manager                  *Manager
	email                    string
	uplink, downlink, active Counter
	online                   atomic.Pointer[OnlineMap]
	enabled                  atomic.Uint32
	changed, deleted         atomic.Bool
	references               uint32
}

func parseUserCounter(name string) (string, uint32, bool) {
	if !strings.HasPrefix(name, "user>>>") {
		return "", 0, false
	}
	rest := name[len("user>>>"):]
	if strings.Contains(rest, ">>>site>>>") {
		return "", 0, false
	}
	for _, suffix := range []struct {
		text string
		kind uint32
	}{{">>>traffic>>>uplink", counterUplink}, {">>>traffic>>>downlink", counterDownlink}, {">>>connections>>>active", counterActive}} {
		if strings.HasSuffix(rest, suffix.text) {
			return strings.TrimSuffix(rest, suffix.text), suffix.kind, true
		}
	}
	return "", 0, false
}
func parseUserOnline(name string) (string, bool) {
	if !strings.HasPrefix(name, "user>>>") || !strings.HasSuffix(name, ">>>online") {
		return "", false
	}
	return strings.TrimSuffix(name[len("user>>>"):], ">>>online"), true
}
func userCounterName(email string, kind uint32) string {
	switch kind {
	case counterUplink:
		return featurestats.UserTrafficCounterName(email, "uplink")
	case counterDownlink:
		return featurestats.UserTrafficCounterName(email, "downlink")
	default:
		return featurestats.ActiveUserConnectionCounterName(email)
	}
}
func (record *userRecord) counter(kind uint32) *Counter {
	switch kind {
	case counterUplink:
		return &record.uplink
	case counterDownlink:
		return &record.downlink
	default:
		return &record.active
	}
}
func (record *userRecord) Uplink() featurestats.Counter {
	if record.enabled.Load()&counterUplink == 0 {
		return nil
	}
	return &record.uplink
}
func (record *userRecord) Downlink() featurestats.Counter {
	if record.enabled.Load()&counterDownlink == 0 {
		return nil
	}
	return &record.downlink
}
func (record *userRecord) Active() featurestats.Counter { return &record.active }
func (record *userRecord) Online() featurestats.OnlineMap {
	if value := record.online.Load(); value != nil {
		return value
	}
	return nil
}
func (record *userRecord) Enable(uplink, downlink, online bool) {
	flags := counterActive
	if uplink {
		flags |= counterUplink
	}
	if downlink {
		flags |= counterDownlink
	}
	if online {
		flags |= counterOnline
		record.ensureOnline()
	}
	record.enabled.Or(flags)
}

func (record *userRecord) ensureOnline() {
	if record.online.Load() != nil {
		return
	}
	value := NewOnlineMap()
	value.owner = record
	record.online.CompareAndSwap(nil, value)
}
func (manager *Manager) newUserRecord(email string) *userRecord {
	record := &userRecord{manager: manager, email: email}
	record.uplink.owner = record
	record.downlink.owner = record
	record.active.owner = record
	return record
}
func (manager *Manager) AcquireUser(email string, uplink, downlink, online bool) (featurestats.UserCounters, error) {
	shard := &manager.userShards[statisticsShard(email)]
	shard.Lock()
	defer shard.Unlock()
	record := shard.entries[email]
	if record == nil {
		record = manager.newUserRecord(email)
		shard.entries[email] = record
	}
	record.references++
	record.Enable(uplink, downlink, online)
	return record, nil
}
func (record *userRecord) Release() {
	manager := record.manager
	shard := &manager.userShards[statisticsShard(record.email)]
	shard.Lock()
	defer shard.Unlock()
	if record.references == 0 {
		return
	}
	record.references--
	if record.references == 0 {
		manager.deleteUserLocked(shard, record)
	}
}
func (manager *Manager) deleteUserLocked(shard *userShard, record *userRecord) {
	record.deleted.Store(true)
	for name := range record.sites {
		counterShard := &manager.counterShards[statisticsShard(name)]
		counterShard.Lock()
		delete(counterShard.entries, name)
		counterShard.Unlock()
	}
	delete(shard.entries, record.email)
	if len(shard.entries) == 0 {
		shard.entries = make(map[string]*userRecord)
	}
	changed := &manager.changedShards[statisticsShard(record.email)]
	changed.Lock()
	if changed.entries[record.email] == record {
		delete(changed.entries, record.email)
	}
	if len(changed.entries) == 0 {
		changed.entries = make(map[string]*userRecord)
	}
	changed.Unlock()
}
func (record *userRecord) markChanged() {
	if record.deleted.Load() || !record.changed.CompareAndSwap(false, true) {
		return
	}
	shard := &record.manager.changedShards[statisticsShard(record.email)]
	shard.Lock()
	if !record.deleted.Load() {
		shard.entries[record.email] = record
	}
	shard.Unlock()
}
func (manager *Manager) VisitChangedUsers(emails []string, visitor func(string, featurestats.UserCounters)) {
	selected := make(map[string]bool, len(emails))
	for _, email := range emails {
		selected[email] = true
	}
	for index := range manager.changedShards {
		shard := &manager.changedShards[index]
		shard.Lock()
		var changed []*userRecord
		for email, record := range shard.entries {
			if len(selected) > 0 && !selected[email] {
				continue
			}
			delete(shard.entries, email)
			record.changed.Store(false)
			changed = append(changed, record)
		}
		shard.Unlock()
		for _, record := range changed {
			if !record.deleted.Load() {
				visitor(record.email, record)
			}
		}
	}
}
func (manager *Manager) userCounter(email string, kind uint32, create, exclusive bool) (featurestats.Counter, error) {
	shard := &manager.userShards[statisticsShard(email)]
	shard.RLock()
	record := shard.entries[email]
	if record != nil && record.enabled.Load()&kind != 0 {
		value := record.counter(kind)
		shard.RUnlock()
		if exclusive {
			return nil, errors.New("Counter ", userCounterName(email, kind), " already registered.")
		}
		return value, nil
	}
	shard.RUnlock()
	if !create {
		return nil, nil
	}
	shard.Lock()
	defer shard.Unlock()
	record = shard.entries[email]
	if record == nil {
		record = manager.newUserRecord(email)
		shard.entries[email] = record
	}
	if exclusive && record.enabled.Load()&kind != 0 {
		return nil, errors.New("Counter ", userCounterName(email, kind), " already registered.")
	}
	record.enabled.Or(kind)
	return record.counter(kind), nil
}
func (manager *Manager) removeUserCounter(email string, kind uint32) {
	shard := &manager.userShards[statisticsShard(email)]
	shard.Lock()
	defer shard.Unlock()
	if record := shard.entries[email]; record != nil {
		record.enabled.And(^kind)
		if record.references == 0 && record.enabled.Load() == 0 {
			manager.deleteUserLocked(shard, record)
		}
	}
}
func (manager *Manager) userOnline(email string, create, exclusive bool) (featurestats.OnlineMap, error) {
	shard := &manager.userShards[statisticsShard(email)]
	shard.RLock()
	record := shard.entries[email]
	if record != nil && record.online.Load() != nil {
		value := record.online.Load()
		shard.RUnlock()
		if exclusive {
			return nil, errors.New("OnlineMap ", email, " already registered.")
		}
		return value, nil
	}
	shard.RUnlock()
	if !create {
		return nil, nil
	}
	shard.Lock()
	defer shard.Unlock()
	record = shard.entries[email]
	if record == nil {
		record = manager.newUserRecord(email)
		shard.entries[email] = record
	}
	if exclusive && record.online.Load() != nil {
		return nil, errors.New("OnlineMap ", email, " already registered.")
	}
	record.ensureOnline()
	record.enabled.Or(counterOnline)
	return record.Online(), nil
}
func (manager *Manager) removeUserOnline(email string) {
	shard := &manager.userShards[statisticsShard(email)]
	shard.Lock()
	defer shard.Unlock()
	if record := shard.entries[email]; record != nil {
		record.online.Store(nil)
		record.enabled.And(^counterOnline)
		if record.references == 0 && record.enabled.Load() == 0 {
			manager.deleteUserLocked(shard, record)
		}
	}
}
