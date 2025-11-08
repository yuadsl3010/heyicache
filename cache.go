package heyicache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

const (
	segCount                     int32 = 256
	segAndOpVal                        = 255
	slotCount                    int32 = 256
	blockCount                   int32 = 10 // cache mode always use 10 blocks
	blockStorageCount            int32 = 1  // storage mode has only 1 block, it can be expanded when storage is unlimited
	minSize                      int64 = 32
	unitMB                       int64 = 1024 * 1024
	defaultEvictionTriggerTiming       = 0.5 // 50%
)

// cache instance, refer to freecache but do more performance optimizations based on arena memory
type Cache struct {
	name               string
	isStorage          bool // true means this cache is used for storage, false means this cache is used for memory cache
	isStorageUnlimited bool // true means storage mode can use unlimited size, false means storage mode will use the MaxSize as limit
	versionStorage     uint32
	locks              [segCount]sync.Mutex
	segments           [segCount]segment
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

	if config.EvictionTriggerTiming < 0 || config.EvictionTriggerTiming > 1 {
		return nil, fmt.Errorf("EvictionTriggerTiming must be in (0, 1]")
	}

	if config.EvictionTriggerTiming == 0 {
		config.EvictionTriggerTiming = defaultEvictionTriggerTiming
	}

	if config.IsStorage && config.VersionStorage == 0 {
		return nil, fmt.Errorf("VersionStorage must be greater than 0 when IsStorage is true")
	}

	cache := &Cache{
		name:               config.Name,
		isStorage:          config.IsStorage,
		isStorageUnlimited: config.IsStorageUnlimited,
		versionStorage:     config.VersionStorage,
	}

	block := blockCount
	if config.IsStorage {
		block = blockStorageCount
	}
	for i := 0; i < int(segCount); i++ {
		cache.segments[i] = newSegment(config.MaxSize*unitMB/int64(segCount), int32(i), config.EvictionTriggerTiming, config.MinWriteInterval, config.CustomTimer, block)
	}

	return cache, nil
}

func (cache *Cache) Name() string {
	if !cache.isStorage {
		return cache.name
	}

	return fmt.Sprintf("%v@%v", cache.name, cache.versionStorage)
}

// useful when you want to create a new storage cache and ignore all old data
func (cache *Cache) NextVersion() uint32 {
	return cache.versionStorage + 1
}

// once call Close(), cache should NOT be used anymore, just wait for the other goroutines to finish their work and recycle the memory
func (cache *Cache) Close() {
	go func() {
		readyForClose := false
		for !readyForClose {
			time.Sleep(time.Second)
			readyForClose = true
			for i := 0; i < int(segCount); i++ {
				cache.locks[i].Lock()
				if !cache.segments[i].readyForClose() {
					cache.locks[i].Unlock()
					readyForClose = false
					break
				}
				cache.locks[i].Unlock()
			}
		}

		for i := 0; i < int(segCount); i++ {
			cache.locks[i].Lock()
			cache.segments[i].close()
			cache.segments[i].slotsData = nil
			cache.locks[i].Unlock()
		}
	}()
}

func (cache *Cache) Set(key []byte, value interface{}, fn HeyiCacheFnIfc, expireSeconds int) error {
	hashVal := hashFunc(key)
	segID := hashVal & segAndOpVal

	cache.locks[segID].Lock()
	err := cache.segments[segID].set(key, value, hashVal, expireSeconds, cache.isStorage, cache.isStorageUnlimited, fn)
	if err != nil {
		cache.segments[segID].writeErrCount += 1
	}
	cache.locks[segID].Unlock()

	return err
}

// Drop the Peek() method, it could be replace by Storage mode if you don't want any data expire or eviction
func (cache *Cache) Get(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	if lease == nil {
		return nil, ErrNilLeaseCtx
	}

	hashVal := hashFunc(key)
	segID := hashVal & segAndOpVal

	cache.locks[segID].Lock()
	segment := &cache.segments[segID]
	value, err := segment.get(key, fn, hashVal)
	if err == nil {
		// why segment.curBlock%blockCount instead of just segment.curBlock?
		// because in storage mode, the segment.curBlock may be greater than blockCount
		blockID := segment.curBlock % blockCount
		// later need to return the lease to keep the used = 0
		segment.bufs[blockID].used += 1
		atomic.AddInt32(&lease.keeps[segID][blockID], 1) // use atomic to avoid the lease being modified by other goroutines
	}
	cache.locks[segID].Unlock()

	return value, err
}

// Del deletes an item in the cache by key and returns true or false if a delete occurred.
func (cache *Cache) Del(key []byte) bool {
	hashVal := hashFunc(key)
	segID := hashVal & segAndOpVal

	cache.locks[segID].Lock()
	affected := cache.segments[segID].del(key, hashVal)
	cache.locks[segID].Unlock()

	return affected
}
