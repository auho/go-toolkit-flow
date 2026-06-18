package producer

import (
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

// Item is a producer-path operator that processes items one by one and produces output.
// Producer path: source → operator (produces data) → destination persistence → exec flow control.
// Exec returns the produced data which is forwarded to a destination.
type Item[SE, DE storage.Entry] interface {
	operator.Operator[SE]

	// Exec processes a single item.
	// Returns the produced items and ok=true if the item was processed, false otherwise.
	Exec(SE) ([]DE, bool, error)

	// PostBatchExec performs batch post-processing on produced items
	// before they are sent to the destination.
	PostBatchExec([]DE) error
}
