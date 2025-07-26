package heyicache

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	segCount   int32 = 256
	slotCount  int32 = 256
	blockCount int32 = 10 // must >= 2
	minSize    int64 = 32
	unitMB     int64 = 1024 * 1024
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

func getSegID(hashVal uint64) uint64 {
	return hashVal % uint64(segCount)
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
		cache.segments[i] = newSegment(config.MaxSize*unitMB/int64(segCount), int32(i), config.MinWriteInterval, config.CustomTimer)
	}

	return cache, nil
}

func (cache *Cache) set(key []byte, value interface{}, fn HeyiCacheFnIfc, expireSeconds int) error {
	hashVal := hashFunc(key)
	segID := getSegID(hashVal)
	valueSize := fn.Size(value, true)

	// create hdr and buffer to write
	cache.locks[segID].Lock()
	err := cache.segments[segID].set(key, value, valueSize, hashVal, expireSeconds, fn)
	cache.locks[segID].Unlock()

	return err
}

func (cache *Cache) Set(key []byte, value interface{}, fn HeyiCacheFnIfc, expireSeconds int) error {
	return cache.set(key, value, fn, expireSeconds)
}

func (cache *Cache) get(lease *Lease, key []byte, fn HeyiCacheFnIfc, peak bool) (interface{}, error) {
	if lease == nil {
		return nil, ErrNilLeaseCtx
	}

	hashVal := hashFunc(key)
	segID := getSegID(hashVal)
	cache.locks[segID].Lock()
	segment := &cache.segments[segID]
	value, err := segment.get(key, fn, hashVal, peak)
	if err == nil {
		// later need to return the lease to keep the used = 0
		segment.update(segment.curBlock, 1)
		lease.keeps[segID][segment.curBlock] += 1
	}
	cache.locks[segID].Unlock()
	return value, err
}

func (cache *Cache) Get(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	return cache.get(lease, key, fn, false)
}

// keep peak feature following the freecache design
func (cache *Cache) Peek(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	return cache.get(lease, key, fn, true)
}

// Del deletes an item in the cache by key and returns true or false if a delete occurred.
func (cache *Cache) Del(key []byte) (affected bool) {
	hashVal := hashFunc(key)
	segID := getSegID(hashVal)
	cache.locks[segID].Lock()
	affected = cache.segments[segID].del(key, hashVal)
	cache.locks[segID].Unlock()
	return
}
