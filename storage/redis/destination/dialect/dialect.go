package dialect

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
)

type Dialect interface {
	DBName() string
	Close() error
	KeyLen(keyName string, keyType redis.KeyType) (int64, error)
	Truncate(keyName string) (int64, error)
	HashSet(keyName string, entries storage.MapEntries) error
	ListPush(keyName string, entries []string) error
	SetAdd(keyName string, entries []string) error
	SortedSetAdd(keyName string, entries storage.ScoreMapEntries) error
}
