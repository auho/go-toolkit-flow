package destination

import (
	"fmt"
	"runtime"
	"time"
)

// BulkConfig holds the configuration for a database Bulk destination
// (batch insert/update via gorm).
type BulkConfig struct {
	IsTruncate      bool
	Concurrency     int
	BatchSize       int64          // number of items per write batch
	TimeoutDuration time.Duration // per-write timeout; defaults to 10s
}

func (c *BulkConfig) getTimeoutDuration() time.Duration {
	if c.TimeoutDuration <= 0 {
		return time.Second * 10
	}

	return c.TimeoutDuration
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *BulkConfig) Check() error {
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size[%d] is invalid", c.BatchSize)
	}

	if c.Concurrency <= 0 {
		c.Concurrency = runtime.NumCPU()
	}

	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = c.getTimeoutDuration()
	}

	return nil
}
