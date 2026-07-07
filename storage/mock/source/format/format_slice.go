package format

import (
	"sync/atomic"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
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

func (f *sliceFormat) Scan(_ string, id *int64, amount int64) (*int64, []storage.SliceEntry) {
	items := make([]storage.SliceEntry, 0, amount)

	startUnixNano := time.Now().UnixNano()
	for i := int64(0); i < amount; i++ {
		newID := atomic.AddInt64(id, 1)
		items = append(items, storage.SliceEntry{newID, startUnixNano + i})
	}

	return id, items
}

func (f *sliceFormat) Copy(items []storage.SliceEntry) []storage.SliceEntry {
	return tool.CopySliceSlice[any](items)
}
