// Package format defines data format interfaces for converting and deep-copying
// query results produced by the source package.
package format

import (
	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/source/dialect"
)

// Format is the data format interface for result conversion and deep copying.
type Format[E storage.Entry] interface {
	QueryByRange(dialect dialect.Dialect, startID, endID int64) ([]E, error)

	// Copy deep-copies the given items.
	Copy(items []E) []E
}
