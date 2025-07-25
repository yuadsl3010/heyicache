package heyicache

type Config struct {
	// required: Name of the cache instance
	Name string

	// MaxSize is a limit for arena size in MB.
	// Once it initialized, it cannot be changed.
	MaxSize int64

	// Minimum seconds interval to write the same key, default is 0, means no limit
	MinWriteInterval int32

	// Custom timer
	CustomTimer Timer
}

func DefaultConfig(name string) Config {
	return Config{
		Name:        name,
		MaxSize:     minSize,
		CustomTimer: defaultTimer{},
	}
}
