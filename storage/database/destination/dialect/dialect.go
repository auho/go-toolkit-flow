package dialect

import "github.com/auho/go-toolkit-flow/v3/storage"

// Dialect is the database dialect interface.
type Dialect interface {
	DBName() string
	Truncate() error
	BulkInsertMap(items storage.MapEntries, batchSize int) error
	BulkInsertSlice(fields []string, items storage.SliceEntries, batchSize int) error
	BulkUpdateMap(idName string, items storage.MapEntries) error
	Close() error
}
