package source

const defaultMockTotal = 100
const defaultMockPageSize = 10

// Config holds the configuration for a mock in-memory source.
type Config struct {
	IDName      string
	PageSize    int64
	Total       int64
	Concurrency int
}

// Check validates the config and applies defaults for missing fields.
// Returns an error if any field has an invalid value.
func (c *Config) Check() error {
	if c.Total <= 0 {
		c.Total = defaultMockTotal
	}

	if c.PageSize <= 0 {
		c.PageSize = defaultMockPageSize
	}

	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}

	if c.IDName == "" {
		c.IDName = "id"
	}

	return nil
}
