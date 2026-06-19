package format

import (
	"github.com/auho/go-toolkit-flow/storage"
)

// Format is the data format interface for the mock destination.
// It describes the write type and provides deep-copy semantics,
// mirroring the format pattern used by database and redis destinations.
type Format[E storage.Entry] interface {
	Type() string
	Copy(items []E) []E
}
