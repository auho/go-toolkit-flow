package consumer

import (
	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/storage"
)

// Batch is a consumer-path processor that processes items in bulk.
// Consumer path: source → processor (no data produced) → exec flow control.
// Exec does not return data; nothing is sent to a destination.
type Batch[SE storage.Entry] interface {
	processor.Processor[SE]

	// Exec processes a batch of items and returns the affected count.
	Exec([]SE) (int64, error)
}
