package destination

type BulkConfig struct {
	IsTruncate  bool
	Concurrency int
	PageSize    int64
}
