package producer

import (
	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/storage"
)

// Batch is a producer-path processor that processes items in bulk and produces output.
// Producer path: source → processor (produces data) → destination persistence → exec flow control.
// Exec returns the produced data which is forwarded to a destination.
//
// Optional: implement AfterBatcher[DE] to perform post-batch processing on
// produced items before they are sent to the destination.
type Batch[SE, DE storage.Entry] interface {
	processor.Processor[SE]

	// Exec processes a batch of items and returns the produced items
	// along with the affected count.
	Exec([]SE) ([]DE, int64, error)
}
