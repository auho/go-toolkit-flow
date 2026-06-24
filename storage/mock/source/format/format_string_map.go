package format

import (
	"strconv"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/tool"
)

var _ Format[storage.StringMapEntry] = (*stringMapFormat)(nil)

type stringMapFormat struct{}

// NewStringMapFormat creates a Format for StringMapEntry data.
func NewStringMapFormat() Format[storage.StringMapEntry] {
	return &stringMapFormat{}
}

func (f *stringMapFormat) Type() string {
	return "stringMap"
}

func (f *stringMapFormat) Scan(idName string, id *int64, amount int64) (*int64, storage.StringMapEntries) {
	items := make(storage.StringMapEntries, amount)

	startString := time.Now().String()
	for i := int64(0); i < amount; i++ {
		item := make(storage.StringMapEntry)
		*id++
		item[idName] = strconv.FormatInt(*id, 10)
		item["content"] = startString + " " + item[idName]
		items[i] = item
	}

	return id, items
}

func (f *stringMapFormat) Copy(items storage.StringMapEntries) storage.StringMapEntries {
	return tool.CopySliceMap[string](items)
}
