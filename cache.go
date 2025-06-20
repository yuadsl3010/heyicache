package heyicache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

const (
	segCount        int32 = 256
	slotCount       int32 = 256
	versionCount    int32 = 2
	segmentAndOpVal       = 255
	minSize         int32 = 32
)

// cache instance, refer to freecache but do more performance optimizations based on arena memory
type Cache struct {
	Name     string
	locks    [segCount]sync.Mutex
	segments [segCount]segment
}

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func NewCache(config Config) (*Cache, error) {
	if len(config.Name) == 0 {
		return nil, fmt.Errorf("cache name cannot be empty")
	}

	if config.MaxSize < minSize {
		return nil, fmt.Errorf("cache size must >= %d MB", minSize)
	}

	if config.CustomTimer == nil {
		config.CustomTimer = defaultTimer{}
	}

	cache := &Cache{
		Name: config.Name,
	}

	for i := 0; i < int(segCount); i++ {
		cache.segments[i] = newSegment(config.MaxSize*1024*1024/segCount, int32(i), config.CustomTimer)
	}

	return cache, nil
}

type FuncSize func(interface{}, bool) int32
type FuncSet func(interface{}, []byte, bool) (interface{}, int32)

func (cache *Cache) Set(key []byte, value interface{}, fnSet FuncSet, fnSize FuncSize, expireSeconds int) error {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	valueSize := fnSize(value, true)

	// allocate space in segment
	cache.locks[segID].Lock()
	segment := &cache.segments[segID]
	segment.used += 1 // keep current buffer not cleaned up
	version := segment.version
	bs, index, err := segment.alloc(key, valueSize)
	if err != nil {
		segment.used -= 1
		cache.locks[segID].Unlock()
		return err
	}

	cache.locks[segID].Unlock()

	// write the key and value into the segment
	segment.write(bs, key, value, fnSet)

	// insert the entry into the segment
	cache.locks[segID].Lock()
	if version != segment.version {
		// segment has been expanded, re-allocate space
		segment.used -= 1
		cache.locks[segID].Unlock()
		return ErrSegmentCleaning
	}

	segment.insert(bs, index, key, valueSize, hashVal, expireSeconds)
	segment.used -= 1
	cache.locks[segID].Unlock()
	return err
}

// after Get() is called, the lease will be kept until Done() is called
type FuncGet func([]byte) interface{}

func (cache *Cache) Get(lease *Lease, key []byte, fnGet FuncGet) (interface{}, error) {
	if lease == nil {
		return nil, ErrNilLeaseCtx
	}

	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	segment := &cache.segments[segID]
	value, err := segment.get(key, fnGet, hashVal, false)
	if err == nil {
		segment.used += 1
		lease.keeps[segID][segment.version] += 1
	}
	cache.locks[segID].Unlock()
	return value, err
}

// Del deletes an item in the cache by key and returns true or false if a delete occurred.
func (cache *Cache) Del(key []byte) (affected bool) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	affected = cache.segments[segID].del(key, hashVal)
	cache.locks[segID].Unlock()
	return
}

// statistics
// EvacuateCount is a metric indicating the number of times an eviction occurred.
func (cache *Cache) EvacuateCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEvacuate)
	}
	return
}

// ExpiredCount is a metric indicating the number of times an expire occurred.
func (cache *Cache) ExpiredCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalExpired)
	}
	return
}

// EntryCount returns the number of items currently in the cache.
func (cache *Cache) EntryCount() (entryCount int64) {
	for i := range cache.segments {
		entryCount += atomic.LoadInt64(&cache.segments[i].entryCount)
	}
	return
}

// HitCount is a metric that returns number of times a key was found in the cache.
func (cache *Cache) HitCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].hitCount)
	}
	return
}

// MissCount is a metric that returns the number of times a miss occurred in the cache.
func (cache *Cache) MissCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].missCount)
	}
	return
}

// LookupCount is a metric that returns the number of times a lookup for a given key occurred.
func (cache *Cache) LookupCount() int64 {
	return cache.HitCount() + cache.MissCount()
}

// HitRate is the ratio of hits over lookups.
func (cache *Cache) HitRate() float64 {
	hitCount, missCount := cache.HitCount(), cache.MissCount()
	lookupCount := hitCount + missCount
	if lookupCount == 0 {
		return 0
	} else {
		return float64(hitCount) / float64(lookupCount)
	}
}

// OverwriteCount indicates the number of times entries have been overriden.
func (cache *Cache) OverwriteCount() (overwriteCount int64) {
	for i := range cache.segments {
		overwriteCount += atomic.LoadInt64(&cache.segments[i].overwrites)
	}
	return
}

// ResetStatistics refreshes the current state of the statistics.
func (cache *Cache) ResetStatistics() {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cache.segments[i].resetStatistics()
		cache.locks[i].Unlock()
	}
}
