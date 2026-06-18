package storage

import "context"

// Destination is the data sink contract for the pipeline.
// It is the downstream exit point: data flows from Source → exec → Destination.
//
// Lifecycle:
//   Prepare → Accept → (async: Receive called by output forwarder) → Done → Finish → Close
//
// Thread safety: each Destination is called serially by a single output-forwarder
// goroutine (one per group). Implementations do not need to be safe for concurrent
// Receive calls unless used inside a MultiDestination with shared sub-destinations.
type Destination[E Entry] interface {
	// Prepare initializes the destination (e.g. opens connections, creates tables).
	Prepare(ctx context.Context) error

	// Accept signals the destination to begin accepting data.
	// Typically starts an internal consumer goroutine that drains the receive channel.
	Accept()

	// Receive processes a batch of items. Called serially by the output forwarder.
	Receive([]E) error

	// Done signals that no more data will be sent.
	// After Done, the destination should finish processing any buffered data.
	Done()

	// Finalizes persistence (e.g. flushes remaining buffered data to the database).
	// Called after Done, once all data has been forwarded.
	Finish() error

	// Close releases resources (e.g. database connections).
	Close() error

	// Summary returns human-readable summary lines for display.
	Summary() []string

	// State returns human-readable state lines for live status display.
	State() []string
}
