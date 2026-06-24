package exec

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

// Runners is a collection of Runner that encapsulates batch lifecycle operations.
// It is the exec-layer analogue of flow.groups for []Runner: all lifecycle
// and data-forwarding logic that iterates over runners lives here.
//
// Concurrency note: the current implementation iterates sequentially.
// Future versions may use errgroup for concurrent execution without
// changing call sites.
type Runners[SE, DE storage.Entry] []Runner[SE, DE]

// NewRunners creates an empty Runners collection.
func NewRunners[SE, DE storage.Entry]() *Runners[SE, DE] {
	r := make(Runners[SE, DE], 0)
	return &r
}

// Add appends one or more runners to the collection.
func (rs *Runners[SE, DE]) Add(r ...Runner[SE, DE]) {
	*rs = append(*rs, r...)
}

// Prepare prepares all runners sequentially. Returns an error on the first failure.
func (rs *Runners[SE, DE]) Prepare(ctx context.Context) error {
	for _, r := range *rs {
		if err := r.Prepare(ctx); err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
	}

	return nil
}

// Start launches all runners' worker goroutines.
func (rs *Runners[SE, DE]) Start() {
	for _, r := range *rs {
		r.Start()
	}
}

// Receive forwards items to all runners (fan-out within this collection).
func (rs *Runners[SE, DE]) Receive(items []SE) {
	for _, r := range *rs {
		r.Receive(items)
	}
}

// Done signals all runners that no more data will be sent.
func (rs *Runners[SE, DE]) Done() {
	for _, r := range *rs {
		r.Done()
	}
}

// Finish waits for all runners to complete. Returns an error on the first failure.
func (rs *Runners[SE, DE]) Finish() error {
	for _, r := range *rs {
		if err := r.Finish(); err != nil {
			return fmt.Errorf("finish: %w", err)
		}
	}

	return nil
}

// Close closes all runners. Returns an error on the first failure.
func (rs *Runners[SE, DE]) Close() error {
	for _, r := range *rs {
		if err := r.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}
	}

	return nil
}

// Summary returns summary lines from all runners.
func (rs *Runners[SE, DE]) Summary() []string {
	lines := make([]string, 0, len(*rs))
	for _, r := range *rs {
		lines = append(lines, r.Summary())
	}

	return lines
}

// State returns state lines from all runners, with each runner's summary
// as a header followed by its state lines.
func (rs *Runners[SE, DE]) State() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Summary())
		for _, s := range r.State() {
			lines = append(lines, "  "+s)
		}
	}

	return lines
}

// Output returns output lines from all runners.
func (rs *Runners[SE, DE]) Output() []string {
	lines := make([]string, 0)
	for _, r := range *rs {
		lines = append(lines, r.Output()...)
	}

	return lines
}

// Len returns the number of runners.
func (rs *Runners[SE, DE]) Len() int {
	return len(*rs)
}

// All returns the underlying runner slice.
func (rs *Runners[SE, DE]) All() []Runner[SE, DE] {
	return *rs
}
