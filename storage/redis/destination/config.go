package destination

type Config struct {
	IsTruncate  bool
	Concurrency int
	PageSize    int64
	Key         string
}
