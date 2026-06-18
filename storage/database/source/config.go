package source

// SectionConfig holds the base configuration for segmented scanning.
type SectionConfig struct {
	Concurrency int
	MaxItems    int64 // maximum number of records to read; 0 means no limit
	StartID     int64 // start ID (inclusive)
	EndID       int64 // end ID (inclusive); 0 means auto-detect
	PageSize    int64 // page size
}
