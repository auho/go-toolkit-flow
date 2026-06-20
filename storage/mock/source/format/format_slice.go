package format

import (
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.SliceEntry] = (*sliceFormat)(nil)

type sliceFormat struct{}

// NewSliceFormat creates a Format for SliceEntry data.
func NewSliceFormat() Format[storage.SliceEntry] {
	return &sliceFormat{}
}

func (f *sliceFormat) Type() string {
	return "slice"
}

func (f *sliceFormat) Scan(idName string, id *int64, amount int64) (*int64, []storage.SliceEntry) {
	items := make([]storage.SliceEntry, 0, amount)

	startUnixNano := time.Now().UnixNano()
	for i := int64(0); i < amount; i++ {
		*id++
		items = append(items, storage.SliceEntry{*id, startUnixNano + i})
	}

	return id, items
}

func (f *sliceFormat) Copy(items []storage.SliceEntry) []storage.SliceEntry {
	return tool.CopySliceSlice[any](items)
}
