package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*insertMapFormat)(nil)

type insertMapFormat struct {
	batchSize int
}

// NewInsertMapFormat creates a format handler that inserts MapEntry items.
func NewInsertMapFormat(batchSize int) Format[storage.MapEntry] {
	return &insertMapFormat{batchSize: batchSize}
}

func (f *insertMapFormat) Write(d dialect.Dialect, items storage.MapEntries) error {
	return d.BulkInsertMap(items, f.batchSize)
}

func (f *insertMapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
