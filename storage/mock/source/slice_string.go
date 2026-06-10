package source

import (
	"strconv"
	"sync/atomic"
	"time"
)

var _ mocker[string] = (*SliceString)(nil)

type SliceString struct {
}

func NewSliceString(config Config) *Mock[string] {
	return newMock[string](config, &SliceString{})
}

func (sm SliceString) scan(idName string, id *int64, amount int64) (*int64, []string) {
	items := make([]string, amount, amount)

	startString := time.Now().String()
	for i := int64(0); i < amount; i++ {
		items[i] = startString + " " + strconv.FormatInt(atomic.AddInt64(id, 1), 10)
	}

	return id, items
}

func (sm SliceString) duplicate(items []string) []string {
	newItems := make([]string, len(items), len(items))
	_ = copy(newItems, items)
	return newItems
}
