package format

import (
	"context"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
)

var _ Format[storage.MapEntry] = (*updateMapFormat)(nil)

type updateMapFormat struct {
	idName string
}

// NewUpdateMapFormat creates a format handler that updates MapEntry items.
func NewUpdateMapFormat(idName string) Format[storage.MapEntry] {
	return &updateMapFormat{idName: idName}
}

func (f *updateMapFormat) Write(ctx context.Context, d dialect.Dialect, items storage.MapEntries) error {
	return d.BulkUpdateMap(ctx, f.idName, items)
}

func (f *updateMapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
