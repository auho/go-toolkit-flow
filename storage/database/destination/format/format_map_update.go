package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*updateMapFormat)(nil)

type updateMapFormat struct {
	idName string
}

// NewUpdateMapFormat 创建 MapEntry 更新格式处理器
func NewUpdateMapFormat(idName string) Format[storage.MapEntry] {
	return &updateMapFormat{idName: idName}
}

func (f *updateMapFormat) Write(d dialect.Dialect, items storage.MapEntries) error {
	return d.BulkUpdateMap(f.idName, items)
}

func (f *updateMapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
