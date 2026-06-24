package format

import (
	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/source/dialect"
	"github.com/auho/go-toolkit-flow/v3/tool"
)

var _ Format[storage.MapEntry] = (*mapFormat)(nil)

type mapFormat struct{}

// NewMapFormat creates a Format for MapEntry data.
func NewMapFormat() Format[storage.MapEntry] {
	return &mapFormat{}
}

func (f *mapFormat) QueryByRange(dialect dialect.Dialect, startID, endID int64) (storage.MapEntries, error) {
	return dialect.QueryMapByRange(startID, endID)
}

func (f *mapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
