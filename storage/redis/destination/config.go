package destination

import "time"

// BulkConfig holds the configuration for a Redis Bulk destination
// (batch write via pipeline).
type BulkConfig struct {
	IsTruncate      bool
	Concurrency     int
	BatchSize       int64 // number of items per write batch
	TimeoutDuration time.Duration
	Key             string
}

func (c *BulkConfig) getTimeoutDuration() time.Duration {
	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = time.Second * 3
	}

	return c.TimeoutDuration
}
