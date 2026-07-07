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
		return time.Second * 3
	}

	return c.TimeoutDuration
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *KeyConfig) Check() error {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}

	if c.PageSize <= 0 {
		c.PageSize = 100
	}

	if c.TimeoutDuration <= 0 {
		c.TimeoutDuration = c.getTimeoutDuration()
	}

	return nil
}
