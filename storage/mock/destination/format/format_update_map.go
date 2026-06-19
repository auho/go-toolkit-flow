package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*updateMapFormat)(nil)

type updateMapFormat struct{}

// NewUpdateMapFormat creates a format handler for MapEntry items (update mode).
func NewUpdateMapFormat() Format[storage.MapEntry] {
	return &updateMapFormat{}
}

func (f *updateMapFormat) Type() string {
	return "updateMap"
}

func (f *updateMapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}
