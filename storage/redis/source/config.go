package source

import "time"

type KeyConfig struct {
	Concurrency     int
	Amount          int64 // 取的总数量，不是精确值
	PageSize        int64
	TimeoutDuration time.Duration
	Key             string
}

func (c *KeyConfig) getTimeoutDuration() time.Duration {
	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = time.Second * 3
	}

	return c.TimeoutDuration
}
