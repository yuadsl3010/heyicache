package heyicache

import "sync/atomic"

// statistics
// EvictionNum is a metric indicating the numbers an eviction occurred.
func (cache *Cache) EvictionNum() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].evictionNum)
	}
	return
}

// EvictionCount is a metric indicating the number of times an eviction was called.
func (cache *Cache) EvictionCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].evictionCount)
	}
	return
}

// EvictionWaitCount is a metric indicating the number of times an eviction wait occurred.
func (cache *Cache) EvictionWaitCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].evictionWaitCount)
	}
	return
}

// ExpiredCount is a metric indicating the number of times an expire occurred.
func (cache *Cache) ExpireCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].expireCount)
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

// GetEntryCap returns the total capacity of all segments in the cache.
func (cache *Cache) EntryCap() (cap int64) {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cap += int64(len(cache.segments[i].slotsData))
		cache.locks[i].Unlock()
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

// ReadCount is a metric that returns the number of times a lookup for a given key occurred.
func (cache *Cache) ReadCount() int64 {
	return cache.HitCount() + cache.MissCount()
}

// WriteCount is a metric that returns the number of times a write to the cache occurred.
func (cache *Cache) WriteCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].writeCount)
	}
	return
}

// WriteErrCount is a metric that returns the number of times a write error occurred.
func (cache *Cache) WriteErrCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].writeErrCount)
	}
	return
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

// MemStat returns the total used and size of all segments in the cache.
func (cache *Cache) MemStat() (used int64, mem int64) {
	for i := range cache.segments {
		cache.locks[i].Lock()
		for _, buf := range &cache.segments[i].bufs {
			used += buf.index
			mem += buf.size
		}
		cache.locks[i].Unlock()
	}
	return
}

// OverwriteCount indicates the number of times entries have been overriden.
func (cache *Cache) OverwriteCount() (overwriteCount int64) {
	for i := range cache.segments {
		overwriteCount += atomic.LoadInt64(&cache.segments[i].overwriteCount)
	}
	return
}

func (cache *Cache) SkipWriteCount() (skipWriteCount int64) {
	for i := range cache.segments {
		skipWriteCount += atomic.LoadInt64(&cache.segments[i].skipWriteCount)
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
