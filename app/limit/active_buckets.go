package limit

import (
	"sync"
	"time"
)

const activeBucketShards = 64

type activeBucketShard struct {
	sync.Mutex
	buckets map[*bucket]struct{}
}

func activeShardNumber(tag string) uint32 {
	hash := uint32(2166136261)
	for index := 0; index < len(tag); index++ {
		hash ^= uint32(tag[index])
		hash *= 16777619
	}
	return hash % activeBucketShards
}
func (runtime *Runtime) activateLocked(bucket *bucket) {
	if bucket.active {
		return
	}
	shard := &runtime.active[activeShardNumber(bucket.tag)]
	shard.Lock()
	shard.buckets[bucket] = struct{}{}
	shard.Unlock()
	bucket.active = true
}
func (runtime *Runtime) activeBuckets() []*bucket {
	var buckets []*bucket
	for index := range runtime.active {
		shard := &runtime.active[index]
		shard.Lock()
		for bucket := range shard.buckets {
			buckets = append(buckets, bucket)
		}
		shard.Unlock()
	}
	return buckets
}
func (runtime *Runtime) removeIdleActive(buckets []*bucket) {
	for _, bucket := range buckets {
		bucket.mu.Lock()
		if bucket.connections == 0 && bucket.up.idleTTL() <= time.Millisecond && bucket.down.idleTTL() <= time.Millisecond {
			shard := &runtime.active[activeShardNumber(bucket.tag)]
			shard.Lock()
			delete(shard.buckets, bucket)
			shard.Unlock()
			bucket.active = false
		}
		bucket.mu.Unlock()
	}
}
func (runtime *Runtime) collectRetired(now time.Time) {
	runtime.mu.Lock()
	retired := runtime.retired
	runtime.retired = nil
	runtime.mu.Unlock()
	var retained, reclaimed []*bucket
	for _, bucket := range retired {
		bucket.mu.Lock()
		bucket.expireIPs(now)
		idle := bucket.connections == 0 && len(bucket.ips) == 0
		since := bucket.idleSince
		bucket.mu.Unlock()
		heavyWindow := time.Duration(bucket.profile.current.Load().GetCongested().GetHeavyWindowSeconds()) * time.Second
		if idle && now.Sub(since) >= heavyWindow && bucket.up.idleTTL() <= time.Millisecond && bucket.down.idleTTL() <= time.Millisecond {
			reclaimed = append(reclaimed, bucket)
		} else {
			retained = append(retained, bucket)
		}
	}
	runtime.mu.Lock()
	runtime.retired = append(runtime.retired, retained...)
	for _, bucket := range reclaimed {
		delete(runtime.buckets, bucket)
		delete(bucket.profile.members, bucket)
	}
	runtime.mu.Unlock()
	for _, bucket := range reclaimed {
		bucket.up.close()
		bucket.down.close()
	}
}
