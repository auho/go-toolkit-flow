package storage

import "context"

// Source is the data source contract for the pipeline.
// It is the upstream entry point: data flows from Source → exec → Destination.
//
// Lifecycle:
//   Prepare → Scan → (async: ReceiveChan consumed by transport) → Finish → Close
//
// Implementations must be safe for the goroutine that calls Scan and the
// goroutine that ranges over ReceiveChan (they may be different).
type Source[E Entry] interface {
	// Prepare initializes the source (e.g. opens connections, creates channels).
	Prepare(ctx context.Context) error

	// Scan launches the producer goroutine that generates data into ReceiveChan.
	// Non-blocking: returns immediately after starting the goroutine.
	Scan()

	// ReceiveChan returns the channel from which downstream consumers read batches.
	// The channel is closed by Finish after all data has been produced.
	ReceiveChan() <-chan []E

	// Finish waits for the scan goroutine to complete and closes ReceiveChan.
	Finish() error

	// Close releases resources (e.g. database connections).
	Close() error

	// Summary returns human-readable summary lines for display.
	Summary() []string

	// StateInfo returns structured state info for external consumers.
	StateInfo() StateInfo

	// Copy creates a deep copy of the given items slice.
	// Used by flow when fan-out to multiple runners requires independent copies
	// to avoid data races on shared data.
	Copy([]E) []E
}
