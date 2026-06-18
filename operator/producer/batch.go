package producer

import (
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

// Batch is a producer-path operator that processes items in bulk and produces output.
// Producer path: source → operator (produces data) → destination persistence → exec flow control.
// Exec returns the produced data which is forwarded to a destination.
type Batch[SE, DE storage.Entry] interface {
	operator.Operator[SE]

	// Exec processes a batch of items and returns the produced items
	// along with the affected count.
	Exec([]SE) ([]DE, int64, error)
}
