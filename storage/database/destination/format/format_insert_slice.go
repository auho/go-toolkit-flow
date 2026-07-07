package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
)

var _ Format[storage.SliceEntry] = (*insertSliceFormat)(nil)

type insertSliceFormat struct {
	fields    []string
	batchSize int // number of items per CreateInBatches call
}

// NewInsertSliceFormat creates a format handler that inserts SliceEntry items.
func NewInsertSliceFormat(fields []string, batchSize int) Format[storage.SliceEntry] {
	return &insertSliceFormat{fields: fields, batchSize: batchSize}
}

func (f *insertSliceFormat) Write(ctx context.Context, d dialect.Dialect, items storage.SliceEntries) error {
	return d.BulkInsertSlice(ctx, f.fields, items, f.batchSize)
}

func (f *insertSliceFormat) Copy(items storage.SliceEntries) storage.SliceEntries {
	return tool.CopySliceSlice[any](items)
}
