package heyicache

type Stat struct {
	EvictionNum       int64   // number of evictions
	EvictionCount     int64   // number of times eviction was called
	ExpandCount       int64   // number of times the segment was expanded
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
	BufCount          int64   // number of buffers in the cache
}

// statistics
func (cache *Cache) GetAndResetStat() *Stat {
	stat := &Stat{}
	for i := range cache.segments {
		cache.locks[i].Lock()
		seg := &cache.segments[i]
		stat.EvictionNum += seg.evictionNum
		stat.EvictionCount += seg.evictionCount
		stat.EvictionWaitCount += seg.evictionWaitCount
		stat.ExpireCount += seg.expireCount
		stat.EntryCount += seg.entryCount
		stat.EntryCap += int64(len(seg.slotsData))
		stat.HitCount += seg.hitCount
		stat.MissCount += seg.missCount
		stat.WriteCount += seg.writeCount
		stat.WriteErrCount += seg.writeErrCount
		stat.OverwriteCount += seg.overwriteCount
		stat.SkipWriteCount += seg.skipWriteCount
		stat.BufCount += int64(len(seg.bufs))
		for _, buf := range seg.bufs {
			stat.MemUsed += buf.index
			stat.MemSize += buf.size
		}
		// clean the statistics
		cache.segments[i].resetStatistics()
		cache.locks[i].Unlock()
	}

	stat.ReadCount = stat.HitCount + stat.MissCount
	return stat
}
