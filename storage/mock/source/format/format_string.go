package format

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/auho/go-toolkit-flow/v3/storage/tool"
)

var _ Format[string] = (*stringFormat)(nil)

type stringFormat struct{}

// NewStringFormat creates a Format for string data.
func NewStringFormat() Format[string] {
	return &stringFormat{}
}

func (f *stringFormat) Type() string {
	return "string"
}

func (f *stringFormat) Scan(_ string, id *int64, amount int64) (*int64, []string) {
	items := make([]string, amount)

	startString := time.Now().String()
	for i := int64(0); i < amount; i++ {
		items[i] = startString + " " + strconv.FormatInt(atomic.AddInt64(id, 1), 10)
	}

	return id, items
}

func (f *stringFormat) Copy(items []string) []string {
	return tool.CopySliceString(items)
}
