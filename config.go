package heyicache

type Config struct {
	// required: Name of the cache instance
	Name string

	// MaxSize is a limit for arena size in MB.
	// Once it initialized, it cannot be changed.
	MaxSize int32

	// If close it, the cache will not allow to exceed MaxSize.
	// default is false, means the cache can grow to MaxSize * (1 + MaxSizeBeyondRatio)
	// we don't recommend to set it to true, because it will cause write error when segment is cleaning
	CloseMaxSizeBeyond bool

	// default is 10% if CloseMaxSizeBeyond is false, means the cache can grow to MaxSize * (1 + MaxSizeBeyondRatio)
	// eg: MaxSize = 100MB, MaxSizeBeyondRatio = 0.1 means the cache can grow to 110MB in short time to decrease write error.
	MaxSizeBeyondRatio float32

	// If close it, the cache will NOT set the buffer start from 0~50% ramdomly
	// it will helpful if you don't want all your buffers expired at the same time
	// default is false
	// we don't recommend to set it to true, because it will cause all your buffers expired at the same time at beginning
	CloseBufferShuffle bool

	// default is 30% if CloseBufferShuffle is false, means the buffer will start from 0~30% ramdomly
	BufferShuffleRatio float32

	// Custom timer
	CustomTimer Timer
}

func DefaultConfig(name string) Config {
	return Config{
		Name:               name,
		MaxSize:            minSize,
		CustomTimer:        defaultTimer{},
		CloseMaxSizeBeyond: defaultCloseMaxSizeBeyond,
		MaxSizeBeyondRatio: defaultMaxSizeBeyondRatio,
		CloseBufferShuffle: defaultCloseBufferShuffle,
		BufferShuffleRatio: defaultBufferShuffleRatio,
	}
}
