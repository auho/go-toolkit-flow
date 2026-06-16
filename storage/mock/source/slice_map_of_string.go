package source

import (
	"strconv"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ generator[storage.MapOfStringsEntry] = (*SliceMapOfString)(nil)

type SliceMapOfString struct {
}

func NewSliceMapOfString(config Config) *Mock[storage.MapOfStringsEntry] {
	return newMock[storage.MapOfStringsEntry](config, &SliceMapOfString{})
}

func (sms SliceMapOfString) scan(idName string, id *int64, amount int64) (*int64, storage.MapOfStringsEntries) {
	items := make([]storage.MapOfStringsEntry, amount)

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

func (sms SliceMapOfString) duplicate(items []storage.MapOfStringsEntry) []storage.MapOfStringsEntry {
	return tool.CopySliceMap(items)
}
