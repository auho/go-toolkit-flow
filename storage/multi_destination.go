package storage

import (
	"context"
	"errors"
)

// MultiDestination fans out to multiple Destinations, analogous to io.MultiWriter.
// All Destination methods iterate through sub-destinations in order.
// Receive and Close use best-effort semantics: all sub-destinations are attempted
// even if one fails, and errors are collected via errors.Join.
// Prepare and Finish use fail-fast semantics: the first error stops iteration.
// Receive deep-copies items for each sub-destination via Copy, so sub-destinations
// receive independent data and can process them concurrently without data races.
// Use for "one runner -> many destinations" (fan-out) scenarios.
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
	var errs []error
	for i, d := range md {
		// Deep-copy items for all sub-destinations except the last one,
		// which receives the original slice. This ensures each sub-destination's
		// async write goroutine operates on independent data.
		var itemsCopy []E
		if i < len(md)-1 {
			itemsCopy = d.Copy(items)
		} else {
			itemsCopy = items
		}
		if err := d.Receive(itemsCopy); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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

func (md MultiDestination[E]) Copy(items []E) []E {
	if len(md) == 0 {
		return nil
	}
	return md[0].Copy(items)
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
