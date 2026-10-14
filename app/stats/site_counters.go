package stats

import (
	"fmt"
	featurestats "github.com/xtls/xray-core/features/stats"
)

type IdentityReleasedError struct{ Email string }

func (err *IdentityReleasedError) Error() string {
	return fmt.Sprintf("stats.identityReleased: %s", err.Email)
}

func (record *userRecord) SiteCounter(name string) (featurestats.Counter, error) {
	shard := &record.manager.userShards[statisticsShard(record.email)]
	shard.RLock()
	defer shard.RUnlock()
	if record.deleted.Load() || shard.entries[record.email] != record {
		return nil, &IdentityReleasedError{Email: record.email}
	}
	counter, err := record.manager.GetOrRegisterCounter(name)
	if err != nil {
		return nil, err
	}
	record.siteAccess.Lock()
	if record.sites == nil {
		record.sites = make(map[string]struct{})
	}
	record.sites[name] = struct{}{}
	record.siteAccess.Unlock()
	return counter, nil
}
