package destination

import "time"

type BulkConfig struct {
	IsTruncate      bool
	Concurrency     int
	PageSize        int64
	TimeoutDuration time.Duration
	Key             string
}

func (c *BulkConfig) getTimeoutDuration() time.Duration {
	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = time.Second * 3
	}

	return c.TimeoutDuration
}
