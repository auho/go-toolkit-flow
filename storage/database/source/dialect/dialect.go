// Package dialect defines the database dialect abstraction used by the source
// package to query data from different database backends.
package dialect

import "github.com/auho/go-toolkit-flow/v3/storage"

// Dialect is the base interface for database dialects.
type Dialect interface {
	DBName() string
	FetchIDBounds() (minID, maxID int64, err error)
	QueryMapByRange(startID, endID int64) (storage.MapEntries, error)
	Close() error
}
