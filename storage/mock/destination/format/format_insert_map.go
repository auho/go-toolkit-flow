package format

import (
	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/tool"
)

var _ Format[storage.MapEntry] = (*insertMapFormat)(nil)

type insertMapFormat struct{}

// NewInsertMapFormat creates a format handler for insert map entries.
func NewInsertMapFormat() Format[storage.MapEntry] {
	return &insertMapFormat{}
}

func (f *insertMapFormat) Type() string {
	return "insertMap"
}

func (f *insertMapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
