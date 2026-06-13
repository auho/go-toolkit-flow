package format

import (
	"github.com/auho/go-toolkit-flow/storage/redis"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

var _ Format[string] = (*setsFormat)(nil)

type setsFormat struct{}

func NewSetsFormat() Format[string] {
	return &setsFormat{}
}

func (f *setsFormat) Write(d dialect.Dialect, keyName string, items []string) error {
	return d.SetAdd(keyName, items)
}

func (f *setsFormat) FetchLen(d dialect.Dialect, keyName string) (int64, error) {
	return d.KeyLen(keyName, redis.KeyTypeSets)
}

func (f *setsFormat) Copy(items []string) []string {
	newItems := make([]string, len(items))
	copy(newItems, items)
	return newItems
}
