package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/redis/source/dialect"
)

type Format[E storage.Entry] interface {
	ScanByRange(d dialect.Dialect, keyName string, cursor int64, count int64) ([]E, int64, error)
	FetchLen(d dialect.Dialect, keyName string) (int64, error)
	Copy(items []E) []E
}
