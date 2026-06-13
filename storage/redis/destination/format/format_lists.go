package format

import (
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[string] = (*listsFormat)(nil)

type listsFormat struct{}

func NewListsFormat() Format[string] {
	return &listsFormat{}
}

func (f *listsFormat) Write(d dialect.Dialect, keyName string, items []string) error {
	return d.ListPush(keyName, items)
}

func (f *listsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeLists)
}

func (f *listsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)
	return newItems
}
