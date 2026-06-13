package dialect

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis"
)

type Dialect interface {
	DBName() string
	Close() error
	KeyLen(keyName string, keyType redis.KeyType) (int64, error)
	HashScan(keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error)
	ListRange(keyName string, start, stop int64) ([]string, error)
	SetScan(keyName string, cursor uint64, count int64) ([]string, uint64, error)
	SortedSetScan(keyName string, cursor uint64, count int64) (storage.MapOfStringsEntries, uint64, error)
	KeyScan(pattern string, cursor uint64, count int64) ([]string, uint64, error)
}
