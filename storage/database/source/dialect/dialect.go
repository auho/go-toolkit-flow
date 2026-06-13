package dialect

import "github.com/auho/go-toolkit-flow/storage"

// Dialect 数据库方言基础接口
type Dialect interface {
	DBName() string
	FetchIDBounds() (minID, maxID int64, err error)
	QueryMapByRange(startID, endID int64) (storage.MapEntries, error)
	Close() error
}
