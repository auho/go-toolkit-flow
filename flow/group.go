package flow

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
)

// group binds a set of runners with one destination.
// Runners' outputs are fan-in merged and then forwarded to the destination.
// destination may be NoopDestination (consumer path), a single Destination, or
// MultiDestination (fan-out to multiple destinations).
// group is an internal type managed by flow and not exported.
type group[SE, DE storage.Entry] struct {
	// runners is a reusable batch collection that supports concurrent iteration in future.
	runners *exec.Runners[SE, DE]
	// destination receives the fan-in merged output from all runners in this group.
	destination storage.Destination[DE]
}

// Prepare prepares this group's runners and destination.
func (g group[SE, DE]) Prepare(ctx context.Context) error {
	if err := g.runners.Prepare(ctx); err != nil {
		return fmt.Errorf("runners.Prepare: %w", err)
	}

	if err := g.destination.Prepare(ctx); err != nil {
		return fmt.Errorf("destination.Prepare: %w", err)
	}

	return nil
}

// Summary returns summary lines for this group's runners and destination.
func (g group[SE, DE]) Summary() []string {
	lines := make([]string, 0)
	lines = append(lines, "    Runners: ")
	for _, s := range g.runners.Summary() {
		lines = append(lines, "      "+s)
	}
	lines = append(lines, "    Destination: ")
	lines = append(lines, g.destination.Summary()...)

	return lines
}

// State returns state lines for this group's runners and destination.
func (g group[SE, DE]) State() []string {
	lines := make([]string, 0)
	lines = append(lines, g.runners.State()...)
	lines = append(lines, g.destination.StateString()...)

	return lines
}

// Close closes this group's runners and destination, collecting all errors.
func (g group[SE, DE]) Close() []error {
	var errs []error

	if err := g.runners.Close(); err != nil {
		errs = append(errs, fmt.Errorf("runners.Close: %w", err))
	}

	if err := g.destination.Close(); err != nil {
		errs = append(errs, fmt.Errorf("destination.Close: %w", err))
	}

	return errs
}