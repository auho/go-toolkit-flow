package format

import (
	"strconv"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ Format[storage.MapOfStringsEntry] = (*mapOfStringFormat)(nil)

type mapOfStringFormat struct{}

// NewMapOfStringFormat creates a Format for MapOfStringsEntry data.
func NewMapOfStringFormat() Format[storage.MapOfStringsEntry] {
	return &mapOfStringFormat{}
}

func (f *mapOfStringFormat) Type() string {
	return "mapOfString"
}

func (f *mapOfStringFormat) Scan(idName string, id *int64, amount int64) (*int64, storage.MapOfStringsEntries) {
	items := make(storage.MapOfStringsEntries, amount)

	startString := time.Now().String()
	for i := int64(0); i < amount; i++ {
		item := make(storage.MapOfStringsEntry)
		*id++
		item[idName] = strconv.FormatInt(*id, 10)
		item["content"] = startString + " " + item[idName]
		items[i] = item
	}

	return id, items
}

func (f *mapOfStringFormat) Copy(items storage.MapOfStringsEntries) storage.MapOfStringsEntries {
	return tool.CopySliceMap[string](items)
}