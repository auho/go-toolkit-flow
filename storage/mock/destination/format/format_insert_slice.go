package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.SliceEntry] = (*insertSliceFormat)(nil)

type insertSliceFormat struct{}

// NewInsertSliceFormat creates a format handler for SliceEntry items.
func NewInsertSliceFormat() Format[storage.SliceEntry] {
	return &insertSliceFormat{}
}

func (f *insertSliceFormat) Type() string {
	return "insertSlice"
}

func (f *insertSliceFormat) Copy(items storage.SliceEntries) storage.SliceEntries {
	return tool.CopySliceSlice[any](items)
}
