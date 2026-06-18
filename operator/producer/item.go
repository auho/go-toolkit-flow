package producer

import (
	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/storage"
)

// Item is a producer-path operator that processes items one by one and produces output.
// Path two: source -> operator (produces data) -> destination persistence -> exec flow control.
// Exec returns the produced data which is forwarded to a destination.
type Item[SE, DE storage.Entry] interface {
	operator.Operator[SE]

	// Exec need to be implemented
	// returns produced items and ok whether the item was processed
	Exec(SE) ([]DE, bool, error)

	// PostBatchExec need to be implemented
	// batch post-processing on produced items before sending to destination
	PostBatchExec([]DE) error
}
