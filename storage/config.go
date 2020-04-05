package storage

const (
	// DefaultCompactThroughput is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. Not that short bursts are allowed
	// to happen at a possibly larger value, set by DefaultCompactThroughputBurst.
	// A value of 0 here will disable compaction rate limiting
	DefaultCompactThroughput = 48 * 1024 * 1024

	// DefaultCompactThroughputBurst is the rate limit in bytes per second that we
	// will allow TSM compactions to write to disk. If this is not set, the burst value
	// will be set to equal the normal throughput
	DefaultCompactThroughputBurst = 48 * 1024 * 102
)
