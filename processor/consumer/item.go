package consumer

import (
	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/storage"
)

// Item is a consumer-path processor that processes items one by one.
// Consumer path: source → processor (no data produced) → exec flow control.
// Exec does not return data; nothing is sent to a destination.
//
// Optional: implement AfterBatcher[SE] to perform post-batch processing on
// the input batch after all items have been processed.
type Item[SE storage.Entry] interface {
	processor.Processor[SE]

	// Exec processes a single item.
	// Returns ok=true if the item was processed, false otherwise.
	Exec(SE) (bool, error)
}
