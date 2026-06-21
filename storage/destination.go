package storage

import "context"

// Destination is the data sink contract for the pipeline.
// It is the downstream exit point: data flows from Source → exec → Destination.
//
// Lifecycle:
//
//	Prepare → Accept → (async: Receive called by output forwarder) → Done → Finish → Close
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

	// Finish Finalizes persistence (e.g. flushes remaining buffered data to the database).
	// Called after Done, once all data has been forwarded.
	Finish() error

	// Close releases resources (e.g. database connections).
	Close() error

	// Summary returns human-readable summary lines for display.
	Summary() []string

	// StateInfo returns structured state info for external consumers.
	StateInfo() State

	// StateString returns a human-readable state string for display.
	StateString() []string
}

// DestinationHolder is optionally implemented by components that hold internal
// destinations written to during processing (e.g. a processor that dispatches
// data to multiple destinations inside Exec). The pipeline discovers these via
// a type assertion and manages their lifecycle uniformly.
//
// The holder itself is responsible for calling dest.Receive during processing;
// this interface only exposes the destinations for lifecycle ownership.
// Holders MUST NOT continue calling dest.Receive after their processing has
// completed, because the pipeline will invoke dest.Done() once workers exit.
type DestinationHolder[DE Entry] interface {
	Destinations() []Destination[DE]
}
