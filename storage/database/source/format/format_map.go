package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*mapFormat)(nil)

type mapFormat struct{}

// NewMapFormat 创建 MapEntry 数据格式处理器
func NewMapFormat() Format[storage.MapEntry] {
	return &mapFormat{}
}

func (f *mapFormat) QueryByRange(dialect dialect.Dialect, startID, endID int64) (storage.MapEntries, error) {
	return dialect.QueryMapByRange(startID, endID)
}

func (f *mapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
