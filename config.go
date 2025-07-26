package heyicache

type Config struct {
	// required: Name of the cache instance
	Name string

	// MaxSize is a limit for arena size in MB.
	// Once it initialized, it cannot be changed.
	MaxSize int64

	// When current block buffer reach EvictionTriggerTiming percent of max size
	// it will trigger the eviction strategy for next block buffer
	// eg: 10 block buffers total, 1~8th blocks are full, 9th is the current block is using, 10th block is also full but the next block
	// if EvictionTriggerTiming is 0.8, then when 9th block buffer reach 80% of max size, which means total use ratio is 98% = 80%(1~8th blocks) + 10%(10th block) + 8%(9th block reach 80% of max size)
	// it will trigger the eviction strategy for 10th block buffer: from now on, all read operations to 10th block buffer will be stopped, and when all read operations in 10th block before has done, 10th block will be evicted, the total use ratio will be decrease to 88%
	// but if the eviction for 10th block is slow(some read operations are still in progress), when 9th block reach full, all write operations will be stopped and return ErrSegmentFull
	// default is 0.5, which means when use ratio reach 95%, it will trigger the eviction and decrease to 85%
	// EvictionTriggerTiming must be in (0, 1], 0 means default 0.5
	EvictionTriggerTiming float32

	// Minimum seconds interval to write the same key, default is 0, means no limit
	MinWriteInterval int32

	// Custom timer
	CustomTimer Timer
}

func DefaultConfig(name string) Config {
	return Config{
		Name:                  name,
		MaxSize:               minSize,
		EvictionTriggerTiming: defaultEvictionTriggerTiming,
		CustomTimer:           defaultTimer{},
	}
}
