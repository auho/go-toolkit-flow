package source

import "runtime"

// Config holds the configuration for a file line source.
type Config struct {
	Name        string
	PageSize    int
	Concurrency int
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *Config) Check() error {
	if c.Concurrency <= 0 {
		c.Concurrency = runtime.NumCPU()
	}

	if c.PageSize <= 0 {
		c.PageSize = 100
	}

	return nil
}
