package heyicache

import (
	"fmt"
	"runtime"
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
	minSize                      int64 = 32
	unitMB                       int64 = 1024 * 1024
	defaultEvictionTriggerTiming       = 0.5 // 50%
	modeZeroCopy                       = 0   // zero copy, don't copy the value, just return the pointer
	modeShallowCopy                    = 1   // shallow copy, copy the value, but the pointer is the same
	modeDeepCopy                       = 2   // deep copy, copy the value, and the pointer is different
)

// cache instance, refer to freecache but do more performance optimizations based on arena memory
type Cache struct {
	Name               string
	leaseName          string
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
		Name:               config.Name,
		leaseName:          config.Name,
		isStorage:          config.IsStorage,
		isStorageUnlimited: config.IsStorageUnlimited,
		versionStorage:     config.VersionStorage,
	}

	if config.IsStorage {
		cache.leaseName = fmt.Sprintf("%v@%v", cache.Name, cache.versionStorage)
	}

	for i := 0; i < int(segCount); i++ {
		cache.segments[i] = newSegment(config.MaxSize*unitMB/int64(segCount), int32(i), config.EvictionTriggerTiming, config.MinWriteInterval, config.CustomTimer)
	}

	return cache, nil
}

// useful when you want to create a new storage cache and ignore all old data
func (cache *Cache) NextVersion() uint32 {
	if cache == nil {
		return 1
	}
	return cache.versionStorage + 1
}

// normally you don't need to close cache manually, becuase when you set your cache pointer to nil, this cache memory will be released by GC
// but if you want to release all memory immediately, you can call this method to close the cache
// after {d} duration calling this method, this cache memory will be released
// only storage mode will need this feature, because it should be an other goroutine update storage daily or hourly, by create a new cache instance, load datas, and switch the requests to new cache
func (cache *Cache) AsyncCloseAfter(d time.Duration) {
	go func() {
		readyForClose := false
		for !readyForClose {
			time.Sleep(d)
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
		runtime.GC()
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

// Actually Get() use lease.Cache instead of cache directly, in some high concurrency scenarios, lease.Cache is old but cache is new when you switch cache instance for storage mode at runtime
// Drop the Peek() method, it could be replace by Storage mode if you don't want any data expire or eviction
func (cache *Cache) get(lease *Lease, key []byte, fn HeyiCacheFnIfc, copyMode int) (interface{}, error) {
	if lease == nil || lease.cache == nil {
		return nil, ErrNilLeaseCtx
	}

	hashVal := hashFunc(key)
	segID := hashVal & segAndOpVal

	lease.cache.locks[segID].Lock()
	segment := &lease.cache.segments[segID]
	value, err := segment.get(key, fn, hashVal)
	if err == nil {
		if copyMode != modeDeepCopy {
			// why segment.curBlock%blockCount instead of just segment.curBlock?
			// because in storage mode, the segment.curBlock may be greater than blockCount
			blockID := segment.curBlock % blockCount
			// later need to return the lease to keep the used = 0
			segment.bufs[blockID].used += 1
			atomic.AddInt32(&lease.keeps[segID][blockID], 1) // use atomic to avoid the lease being modified by other goroutines

			if copyMode == modeShallowCopy {
				shallow := fn.New(true)
				fn.ShallowCopy(value, shallow)
				value = shallow
				// for shallow copy, use obj pool to reuse the object
				lease.mutex.Lock()
				if lease.objs == nil {
					lease.objs = make(map[HeyiCacheFnIfc][]interface{})
				}
				lease.objs[fn] = append(lease.objs[fn], shallow)
				lease.mutex.Unlock()
			}
		} else {
			// deep copy don't need to keep the lease cause the value is copied
			deep := fn.New(false)
			fn.DeepCopy(value, deep)
			value = deep
		}
	}

	lease.cache.locks[segID].Unlock()
	return value, err
}

// the performance is GetZeroCopy >= GetShallowCopy > GetDeepCopy > freecache / bigcache which used proto marshal/unmarshal
// compare to GetZeroCopy, GetShallowCopy will allocate a new object so it will have faster memory allocation speed

// defualt mode is shallow copy
// it will return a new object pointer, the inner non-pointer fields will be copied, but the pointer fields will point to the cache []byte memory space directly
// though you can't modify the pointer fields, you can modify the non-pointer fields
func (cache *Cache) Get(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	return cache.get(lease, key, fn, modeShallowCopy)
}

// zero copy mode is more aggressive than shallow copy mode
// it will return the original pointer to the cache []byte memory space directly
// if you won't modify any value just for pure reading, and you need extremely performance, you can use this mode
// PS. actually modify non-pointer fields is fine but not recommended because protobuf marshal will panic, eg:
// 1. assume the struct just include two uint64 fields, and the values is (0, 2)
// 2. goroutine-1 start marshal, alloc a new []byte memory space, which len is 8 bytes, because the 0 value will not be marshaled into the []byte memory space
// 3. goroutine-2 start modify, change value to (1, 2)
// 4. goroutine-1 continue marshal, use []byte memory marshal the first value 1, it spent 8 bytes, and continue marshal the second value 2, it will panic because there are no enough memory space
// so you can use shallow copy mode in this case and still have good performance
func (cache *Cache) GetZeroCopy(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	return cache.get(lease, key, fn, modeZeroCopy)
}

// deep copy mode is the most safe mode but also the most performance-consuming mode
// it will return a new object pointer, all fields will be copied
// so you can modify anything as you want
func (cache *Cache) GetDeepCopy(lease *Lease, key []byte, fn HeyiCacheFnIfc) (interface{}, error) {
	return cache.get(lease, key, fn, modeDeepCopy)
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
