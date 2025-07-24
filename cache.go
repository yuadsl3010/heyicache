package heyicache

import (
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	segCount                  int32   = 2048
	slotCount                 int32   = 256
	versionCount              int32   = 2
	minSize                   int32   = 32
	defaultCloseMaxSizeBeyond bool    = false
	defaultMaxSizeBeyondRatio float32 = 0.1
	defaultCloseBufferShuffle bool    = false
	defaultBufferShuffleRatio float32 = 0.3
)

// cache instance, refer to freecache but do more performance optimizations based on arena memory
type Cache struct {
	Name     string
	locks    [segCount]sync.Mutex
	segments [segCount]segment
	idleBuf  int32
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

	if !config.CloseMaxSizeBeyond {
		config.MaxSizeBeyondRatio = defaultMaxSizeBeyondRatio
	}

	if !config.CloseBufferShuffle {
		config.BufferShuffleRatio = defaultBufferShuffleRatio
	}

	cache := &Cache{
		Name:    config.Name,
		idleBuf: int32(float32(segCount) * config.MaxSizeBeyondRatio),
	}

	for i := 0; i < int(segCount); i++ {
		cache.segments[i] = newSegment(config.MaxSize*1024*1024/segCount, int32(i), &cache.idleBuf, config.BufferShuffleRatio, config.CustomTimer)
	}

	return cache, nil
}

func (cache *Cache) set(key []byte, value interface{}, fn HeyiCacheFnIfc, expireSeconds int, canRetry bool) error {
	hashVal := hashFunc(key)
	segID := getSegID(hashVal)
	valueSize := fn.Size(value, true)

	// new a hdr
	cache.locks[segID].Lock()
	segment := &cache.segments[segID]
	version := segment.version
	segment.processUsed(version, 1) // keep current buffer not cleaned up
	hdr, bs, err := segment.newHdr(version, key, valueSize, hashVal, expireSeconds)
	if err != nil {
		segment.processUsed(version, -1)
		cache.locks[segID].Unlock()
		if err == ErrSegmentCleaning && canRetry {
			// give one more chance to retry
			return cache.set(key, value, fn, expireSeconds, false)
		}

		return err
	}

	cache.locks[segID].Unlock()

	// write the key and value into the segment
	// assume fnSet will take lots of time, so we should not hold the lock
	segment.write(bs, key, value, fn)

	// insert the entry into the segment
	cache.locks[segID].Lock()
	segment.processUsed(version, -1)
	if version != segment.version {
		// segment has been expanded, re-allocate space
		cache.locks[segID].Unlock()
		if canRetry {
			// give one more chance to retry
			return cache.set(key, value, fn, expireSeconds, false)
		}
		return ErrSegmentCleaning
	}

	// update header
	hdr.deleted = false // mark as not deleted
	cache.locks[segID].Unlock()
	return err
}

func (cache *Cache) Set(key []byte, value interface{}, fn HeyiCacheFnIfc, expireSeconds int) error {
	return cache.set(key, value, fn, expireSeconds, true)
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
		segment.processUsed(segment.version, 1)
		lease.keeps[segID][segment.version] += 1
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
