package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
)

// Format 数据格式接口，负责写入和深拷贝
type Format[E storage.Entry] interface {
	Write(dialect dialect.Dialect, items []E) error
	Copy(items []E) []E
}
