package source

// Config holds the configuration for a mock in-memory source.
type Config struct {
	IDName      string
	PageSize    int64
	Total       int64
	Concurrency int
}
