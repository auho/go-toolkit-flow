package source

import (
	"fmt"
	"runtime"
	"time"
)

// SectionConfig holds the base configuration for segmented scanning.
type SectionConfig struct {
	Concurrency     int
	MaxItems        int64 // maximum number of records to read; 0 means no limit
	StartID         int64 // start ID (inclusive)
	EndID           int64 // end ID (inclusive); 0 means auto-detect
	PageSize        int64 // page size
	TimeoutDuration time.Duration // per-query timeout; defaults to 10s
}

func (c *SectionConfig) getTimeoutDuration() time.Duration {
	if c.TimeoutDuration <= 0 {
		return time.Second * 10
	}

	return c.TimeoutDuration
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *SectionConfig) Check() error {
	if c.PageSize <= 0 {
		return fmt.Errorf("page size[%d] is invalid", c.PageSize)
	}

	if c.MaxItems < 0 {
		return fmt.Errorf("max items[%d] is negative", c.MaxItems)
	}

	if c.Concurrency <= 0 {
		c.Concurrency = runtime.NumCPU()
	}

	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = c.getTimeoutDuration()
	}

	return nil
}
