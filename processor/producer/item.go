package producer

import (
	"github.com/auho/go-toolkit-flow/v3/processor"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// Item is a producer-path processor that processes items one by one and produces output.
// Producer path: source → processor (produces data) → destination persistence → exec flow control.
// Exec returns the produced data which is forwarded to a destination.
//
// Optional: implement AfterBatcher[DE] to perform post-batch processing on
// produced items before they are sent to the destination.
type Item[SE, DE storage.Entry] interface {
	processor.Processor[SE]

	// Exec processes a single item.
	// Returns the produced items and ok=true if the item was processed, false otherwise.
	Exec(SE) ([]DE, bool, error)
}
