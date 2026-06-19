package storage

import "context"

// NoopDestination is a no-op Destination implementation (Null Object pattern).
// It is used as the default destination when no real destination is configured
// (path one: consumer mode), so that flow never needs nil checks.
type NoopDestination[E Entry] struct{}

// Compile-time interface conformance check.
var _ Destination[string] = NoopDestination[string]{}

func (NoopDestination[E]) Prepare(context.Context) error { return nil }
func (NoopDestination[E]) Accept()                       {}
func (NoopDestination[E]) Receive([]E) error             { return nil }
func (NoopDestination[E]) Done()                         {}
func (NoopDestination[E]) Finish() error                 { return nil }
func (NoopDestination[E]) Close() error                  { return nil }
func (NoopDestination[E]) Summary() []string             { return nil }
func (NoopDestination[E]) StateInfo() StateInfo           { return nil }
