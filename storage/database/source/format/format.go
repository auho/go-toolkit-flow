package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
)

// Format Format[E] 数据格式接口，负责结果转换和深拷贝
type Format[E storage.Entry] interface {
	QueryByRange(dialect dialect.Dialect, startID, endID int64) ([]E, error)

	// Copy 深拷贝数据
	Copy(items []E) []E
}
