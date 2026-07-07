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
		return time.Second * 3
	}

	return c.TimeoutDuration
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *BulkConfig) Check() error {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 20
	}

	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = c.getTimeoutDuration()
	}

	return nil
}
