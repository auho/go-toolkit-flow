package format

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
)

// Format is the data format interface, responsible for writing and deep copying.
type Format[E storage.Entry] interface {
	Write(dialect dialect.Dialect, items []E) error
	Copy(items []E) []E
}
