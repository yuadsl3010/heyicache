package heyicache

import "sync/atomic"

// statistics
// EvictionCount is a metric indicating the numbers an eviction occurred.
func (cache *Cache) EvictionCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEviction)
	}
	return
}

// EvictionCall is a metric indicating the number of times an eviction was called.
func (cache *Cache) EvictionCall() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEvictionCount)
	}
	return
}

// EvictionWaitCount is a metric indicating the number of times an eviction wait occurred.
func (cache *Cache) EvictionWaitCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEvictionWait)
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

// UsedStat returns the total used and size of all segments in the cache.
func (cache *Cache) UsedStat() (int32, int32) {
	var totalUsed int32
	var totalSize int32
	for i := range cache.segments {
		cache.locks[i].Lock()
		buf := cache.segments[i].getBuffer()
		totalUsed += buf.index
		totalSize += buf.size
		cache.locks[i].Unlock()
	}
	return totalUsed, totalSize
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
