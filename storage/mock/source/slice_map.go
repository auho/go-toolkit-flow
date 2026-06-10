package source

import (
	"sync/atomic"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ generator[storage.MapEntry] = (*SliceMap)(nil)

type SliceMap struct {
}

func NewSliceMap(config Config) *Mock[storage.MapEntry] {
	return newMock[storage.MapEntry](config, &SliceMap{})
}

func (sm SliceMap) scan(idName string, id *int64, amount int64) (*int64, storage.MapEntries) {
	items := make([]storage.MapEntry, amount, amount)

	startUnixNano := time.Now().UnixNano()
	for i := int64(0); i < amount; i++ {
		item := make(storage.MapEntry)
		atomic.AddInt64(id, 1)
		item[idName] = *id
		item["content"] = startUnixNano + i
		items[i] = item
	}

	return id, items
}

func (sm SliceMap) duplicate(items []storage.MapEntry) []storage.MapEntry {
	return tool.CopySliceMap(items)
}
