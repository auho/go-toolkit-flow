package dialect

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

// Dialect is the database dialect interface.
type Dialect interface {
	DBName() string
	Truncate(ctx context.Context) error
	BulkInsertMap(ctx context.Context, items storage.MapEntries, batchSize int) error
	BulkInsertSlice(ctx context.Context, fields []string, items storage.SliceEntries, batchSize int) error
	BulkUpdateMap(ctx context.Context, idName string, items storage.MapEntries) error
	Close() error
}
