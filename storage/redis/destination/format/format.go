package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/destination/dialect"
)

type Format[E storage.Entry] interface {
	Write(dialect dialect.Dialect, keyName string, items []E) error
	FetchLen(dialect dialect.Dialect, keyName string) (int64, error)
	Copy(items []E) []E
}
