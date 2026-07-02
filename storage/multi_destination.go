package storage

import (
	"context"
	"errors"
)

// MultiDestination fans out to multiple Destinations, analogous to io.MultiWriter.
// All Destination methods iterate through sub-destinations in order.
// Receive forwards items sequentially; if any sub-destination returns an error,
// the remaining sub-destinations are skipped and the error is propagated immediately.
// Use for "one runner → many destinations" (fan-out) scenarios.
// For consumer paths with no destination, use NoopDestination instead.
type MultiDestination[E Entry] []Destination[E]

// Compile-time interface conformance check.
var _ Destination[string] = MultiDestination[string]{}

func (md MultiDestination[E]) Prepare(ctx context.Context) error {
	for _, d := range md {
		if err := d.Prepare(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (md MultiDestination[E]) Accept() {
	for _, d := range md {
		d.Accept()
	}
}

func (md MultiDestination[E]) Receive(items []E) error {
	for _, d := range md {
		if err := d.Receive(items); err != nil {
			return err
		}
	}

	return nil
}

func (md MultiDestination[E]) Done() {
	for _, d := range md {
		d.Done()
	}
}

func (md MultiDestination[E]) Finish() error {
	for _, d := range md {
		if err := d.Finish(); err != nil {
			return err
		}
	}

	return nil
}

func (md MultiDestination[E]) Close() error {
	var errs []error
	for _, d := range md {
		if err := d.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (md MultiDestination[E]) Summary() []string {
	lines := make([]string, 0)
	for _, d := range md {
		lines = append(lines, d.Summary()...)
	}

	return lines
}

func (md MultiDestination[E]) State() State {
	states := make([]State, 0, len(md))
	for _, d := range md {
		states = append(states, d.State())
	}
	return NewMultiSnapshot(states)
}

func (md MultiDestination[E]) StateString() []string {
	return []string{md.State().Overview()}
}
