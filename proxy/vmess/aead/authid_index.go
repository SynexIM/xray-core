package aead

import (
	"container/list"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

const decoderSegments = 64
const sourceCacheCapacity = 65536

type decoderSegment struct {
	sync.RWMutex
	entries     map[[16]byte]*AuthIDDecoderItem
	recent      list.List
	lastSuccess atomic.Uint64
}

type decoderIndex struct {
	segments    [decoderSegments]decoderSegment
	sources     sync.Map
	sourceCount atomic.Uint32
	sequence    atomic.Uint64
}

func newDecoderIndex() *decoderIndex {
	index := new(decoderIndex)
	for segment := range index.segments {
		index.segments[segment].entries = make(map[[16]byte]*AuthIDDecoderItem)
	}
	return index
}
func (index *decoderIndex) add(key [16]byte, ticket interface{}) {
	segmentNumber := key[0] % decoderSegments
	segment := &index.segments[segmentNumber]
	segment.Lock()
	defer segment.Unlock()
	if previous := segment.entries[key]; previous != nil {
		previous.removed.Store(true)
		segment.recent.Remove(previous.position)
	}
	item := NewAuthIDDecoderItem(key, ticket)
	item.segment = segmentNumber
	item.position = segment.recent.PushFront(item)
	segment.entries[key] = item
}
func (index *decoderIndex) remove(key [16]byte) {
	segment := &index.segments[key[0]%decoderSegments]
	segment.Lock()
	defer segment.Unlock()
	if item := segment.entries[key]; item != nil {
		item.removed.Store(true)
		segment.recent.Remove(item.position)
		delete(segment.entries, key)
	}
}
func (index *decoderIndex) remember(source string, item *AuthIDDecoderItem) {
	segment := &index.segments[item.segment]
	segment.Lock()
	if !item.removed.Load() {
		segment.recent.MoveToFront(item.position)
		segment.lastSuccess.Store(index.sequence.Add(1))
	}
	segment.Unlock()
	if source == "" {
		return
	}
	if _, found := index.sources.Load(source); found {
		index.sources.Store(source, weak.Make(item))
		return
	}
	// 来源是外部输入，不能让一次性来源地址无限占用缓存；缓存不拥有已删除的身份。
	if index.sourceCount.Load() >= sourceCacheCapacity {
		return
	}
	if _, loaded := index.sources.LoadOrStore(source, weak.Make(item)); !loaded {
		index.sourceCount.Add(1)
	}
}

func decodeTimestamp(item *AuthIDDecoderItem, authID, decoded []byte) (int64, bool) {
	item.dec.s.Decrypt(decoded, authID)
	if binary.BigEndian.Uint32(decoded[12:]) != crc32.ChecksumIEEE(decoded[:12]) {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(decoded)), true
}

func (holder *AuthIDDecoderHolder) matchSource(authID [16]byte, source string) (interface{}, error) {
	var decoded [16]byte
	var matched *AuthIDDecoderItem
	var timestamp int64
	if source != "" {
		if cached, found := holder.index.sources.Load(source); found {
			item := cached.(weak.Pointer[AuthIDDecoderItem]).Value()
			if item != nil && !item.removed.Load() {
				if decodedTime, valid := decodeTimestamp(item, authID[:], decoded[:]); valid {
					matched = item
					timestamp = decodedTime
				}
			}
			if item == nil || item.removed.Load() {
				if _, removed := holder.index.sources.LoadAndDelete(source); removed {
					holder.index.sourceCount.Add(^uint32(0))
				}
			}
		}
	}
	var order [decoderSegments]int
	if matched == nil {
		for index := range order {
			order[index] = index
		}
		slices.SortFunc(order[:], func(first, second int) int {
			firstTime := holder.index.segments[first].lastSuccess.Load()
			secondTime := holder.index.segments[second].lastSuccess.Load()
			if firstTime > secondTime {
				return -1
			}
			if firstTime < secondTime {
				return 1
			}
			return first - second
		})
	}
	for offset := 0; matched == nil && offset < decoderSegments; offset++ {
		segment := &holder.index.segments[order[offset]]
		segment.RLock()
		for position := segment.recent.Front(); position != nil; position = position.Next() {
			item := position.Value.(*AuthIDDecoderItem)
			if decodedTime, valid := decodeTimestamp(item, authID[:], decoded[:]); valid {
				matched = item
				timestamp = decodedTime
				break
			}
		}
		segment.RUnlock()
	}
	if matched == nil {
		return nil, ErrNotFound
	}
	if timestamp < 0 {
		return nil, ErrNeagtiveTime
	}
	now := time.Now().Unix()
	if timestamp < now-120 || timestamp > now+120 {
		return nil, ErrInvalidTime
	}
	if !holder.filter.Check(authID) {
		return nil, ErrReplay
	}
	if matched.removed.Load() {
		return nil, ErrNotFound
	}
	holder.index.remember(source, matched)
	return matched.ticket, nil
}
