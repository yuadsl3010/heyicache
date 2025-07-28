package heyicache

import "sync/atomic"

type Stat struct {
	EvictionNum       int64   // number of evictions
	EvictionCount     int64   // number of times eviction was called
	EvictionWaitCount int64   // number of times eviction wait occurred
	ExpireCount       int64   // number of times an expire occurred
	EntryCount        int64   // number of items currently in the cache
	EntryCap          int64   // total capacity of all segments in the cache
	HitCount          int64   // number of times a key was found in the cache
	MissCount         int64   // number of times a miss occurred in the cache
	ReadCount         int64   // number of times a lookup for a given key occurred
	WriteCount        int64   // number of times a write to the cache occurred
	WriteErrCount     int64   // number of times a write error occurred
	HitRate           float64 // ratio of hits over lookups
	MemUsed           int64   // total used memory in the cache
	MemSize           int64   // total size of all segments in the cache
	OverwriteCount    int64   // number of times entries have been overriden
	SkipWriteCount    int64   // number of times a write was skipped due to MinWriteInterval
}

// statistics
func (cache *Cache) GetAndResetStat() *Stat {
	stat := &Stat{}
	for i := range cache.segments {
		cache.locks[i].Lock()
		stat.EvictionNum += atomic.LoadInt64(&cache.segments[i].evictionNum)
		stat.EvictionCount += atomic.LoadInt64(&cache.segments[i].evictionCount)
		stat.EvictionWaitCount += atomic.LoadInt64(&cache.segments[i].evictionWaitCount)
		stat.ExpireCount += atomic.LoadInt64(&cache.segments[i].expireCount)
		stat.EntryCount += atomic.LoadInt64(&cache.segments[i].entryCount)
		stat.EntryCap += int64(len(cache.segments[i].slotsData))
		stat.HitCount += atomic.LoadInt64(&cache.segments[i].hitCount)
		stat.MissCount += atomic.LoadInt64(&cache.segments[i].missCount)
		stat.ReadCount += stat.HitCount + stat.MissCount
		stat.WriteCount += atomic.LoadInt64(&cache.segments[i].writeCount)
		stat.WriteErrCount += atomic.LoadInt64(&cache.segments[i].writeErrCount)
		stat.OverwriteCount += atomic.LoadInt64(&cache.segments[i].overwriteCount)
		stat.SkipWriteCount += atomic.LoadInt64(&cache.segments[i].skipWriteCount)
		for _, buf := range &cache.segments[i].bufs {
			stat.MemUsed += buf.index
			stat.MemSize += buf.size
		}
		// clean the statistics
		cache.segments[i].resetStatistics()
		cache.locks[i].Unlock()
	}
	return stat
}
