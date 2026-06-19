package format

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

var _ Format[string] = (*stringFormat)(nil)

type stringFormat struct{}

// NewStringFormat creates a Format for string data.
func NewStringFormat() Format[string] {
	return &stringFormat{}
}

func (f *stringFormat) Type() string {
	log.Println("[format] string.Type() -> string")
	return "string"
}

func (f *stringFormat) Scan(idName string, id *int64, amount int64) (*int64, []string) {
	startID := *id
	log.Printf("[format] string.Scan: start id=%d, idName=%s, amount=%d", startID, idName, amount)

	items := make([]string, amount)

	startString := time.Now().String()
	for i := int64(0); i < amount; i++ {
		items[i] = startString + " " + strconv.FormatInt(atomic.AddInt64(id, 1), 10)
	}

	log.Printf("[format] string.Scan: generated %d items, end id=%d", len(items), *id)
	return id, items
}

func (f *stringFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	_ = copy(newItems, items)
	return newItems
}