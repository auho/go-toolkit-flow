package source

import "time"

// KeyConfig holds the configuration for a Redis key-based source
// (hashes/lists/sets/sorted sets/scan).
type KeyConfig struct {
	Concurrency     int
	Amount          int64 // total amount to fetch; not an exact value
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
