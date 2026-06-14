package source

import "time"

type KeyConfig struct {
	Concurrency     int
	Amount          int64 // 取的总数量，不是精确值
	PageSize        int64
	TimeOutDuration time.Duration
	KeyName         string
}

func (c *KeyConfig) GetTimeOutDuration() time.Duration {
	if c.TimeOutDuration <= 0 {
		c.TimeOutDuration = time.Second * 3
	}

	return c.TimeOutDuration
}
