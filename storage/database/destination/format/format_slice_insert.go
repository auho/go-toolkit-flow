package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.SliceEntry] = (*insertSliceFormat)(nil)

type insertSliceFormat struct {
	fields    []string
	batchSize int
}

// NewInsertSliceFormat 创建 SliceEntry 插入格式处理器
func NewInsertSliceFormat(fields []string, batchSize int) Format[storage.SliceEntry] {
	return &insertSliceFormat{fields: fields, batchSize: batchSize}
}

func (f *insertSliceFormat) Write(d dialect.Dialect, items storage.SliceEntries) error {
	return d.BulkInsertSlice(f.fields, items, f.batchSize)
}

func (f *insertSliceFormat) Copy(items storage.SliceEntries) storage.SliceEntries {
	return tool.CopySliceSlice[any](items)
}
