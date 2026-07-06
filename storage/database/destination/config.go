package destination

// BulkConfig holds the configuration for a database Bulk destination
// (batch insert/update via gorm).
type BulkConfig struct {
	IsTruncate  bool
	Concurrency int
	BatchSize   int64 // number of items per write batch
}
