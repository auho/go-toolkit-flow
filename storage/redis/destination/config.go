package destination

import "time"

type BulkConfig struct {
	IsTruncate      bool
	Concurrency     int
	PageSize        int64
	TimeOutDuration time.Duration
	KeyName         string
}

func (c *BulkConfig) GetTimeOutDuration() time.Duration {
	if c.TimeOutDuration <= 0 {
		c.TimeOutDuration = time.Second * 3
	}

	return c.TimeOutDuration
}
