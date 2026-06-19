package format

import (
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapEntry] = (*mapFormat)(nil)

type mapFormat struct{}

// NewMapFormat creates a Format for MapEntry data.
func NewMapFormat() Format[storage.MapEntry] {
	return &mapFormat{}
}

func (f *mapFormat) Type() string {
	return "map"
}

func (f *mapFormat) Scan(idName string, id *int64, amount int64) (*int64, storage.MapEntries) {
	items := make(storage.MapEntries, amount)

	startUnixNano := time.Now().UnixNano()
	for i := int64(0); i < amount; i++ {
		item := make(storage.MapEntry)
		*id++
		item[idName] = *id
		item["content"] = startUnixNano + i
		items[i] = item
	}

	return id, items
}

func (f *mapFormat) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[any](items)
}