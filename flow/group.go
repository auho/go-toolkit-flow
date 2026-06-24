package flow

import (
	"context"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/v3/exec"
	"github.com/auho/go-toolkit-flow/v3/storage"
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
	// internalDests holds destinations owned by runners' processors (via
	// storage.DestinationHolder). Collected once in Prepare (after runners'
	// Prepare succeeds) and wrapped as a storage.Destination (NoopDestination
	// when empty, MultiDestination otherwise); flow manages their lifecycle
	// uniformly with destination.
	internalDests storage.Destination[DE]
}

// newGroup constructs a group from runners and a destination. Internal
// destinations held by the runners' processors are NOT collected here; they
// are discovered in Prepare after runners' Prepare succeeds, because a
// processor may populate its destinations during Prepare.
func newGroup[SE, DE storage.Entry](rs *exec.Runners[SE, DE], dest storage.Destination[DE]) *group[SE, DE] {
	return &group[SE, DE]{
		runners:       rs,
		destination:   dest,
		internalDests: storage.NoopDestination[DE]{},
	}
}

// Start launches this group's runners' worker goroutines, then signals the
// destination and internal destinations to accept data.
func (g *group[SE, DE]) Start() {
	g.runners.Start()
	g.destination.Accept()
	g.internalDests.Accept()
}

// Done signals this group's runners that no more data will be sent.
func (g *group[SE, DE]) Done() {
	g.runners.Done()
}

// Finish waits for this group's runners to complete processing, then signals
// Done on the internal destinations (safe because workers have exited).
// Internally, runners.Finish waits for all runner goroutines to exit and then
// closes each runner's OutChan.
func (g *group[SE, DE]) Finish() error {
	if err := g.runners.Finish(); err != nil {
		return fmt.Errorf("runners.Finish: %w", err)
	}

	g.internalDests.Done()

	return nil
}

// DestinationFinish finalizes persistence for this group's destination and
// internal destinations. Called after all data has been forwarded and Done.
func (g *group[SE, DE]) DestinationFinish() error {
	if err := g.destination.Finish(); err != nil {
		return fmt.Errorf("destination.Finish: %w", err)
	}

	if err := g.internalDests.Finish(); err != nil {
		return fmt.Errorf("internal destination.Finish: %w", err)
	}

	return nil
}

// Output returns output lines from this group's runners.
func (g *group[SE, DE]) Output() []string {
	return g.runners.Output()
}

// Prepare prepares this group's runners, destination, and internal destinations.
// Internal destinations are collected from runners after their Prepare succeeds,
// because a processor may populate its destinations during Prepare.
func (g *group[SE, DE]) Prepare(ctx context.Context) error {
	if err := g.runners.Prepare(ctx); err != nil {
		return fmt.Errorf("runners.Prepare: %w", err)
	}

	// Collect internal destinations held by runners' processors now that
	// runners' Prepare has completed. Wrap as MultiDestination when any are
	// held; otherwise leave the NoopDestination assigned in newGroup.
	var md storage.MultiDestination[DE]
	for _, r := range g.runners.All() {
		if dh, ok := r.(storage.DestinationHolder[DE]); ok {
			md = append(md, dh.Destinations()...)
		}
	}
	if len(md) > 0 {
		g.internalDests = md
	}

	if err := g.destination.Prepare(ctx); err != nil {
		return fmt.Errorf("destination.Prepare: %w", err)
	}

	if err := g.internalDests.Prepare(ctx); err != nil {
		return fmt.Errorf("internal destination.Prepare: %w", err)
	}

	return nil
}

// Summary returns summary lines for this group's runners, destination, and
// any internal destinations held by runners.
func (g *group[SE, DE]) Summary() []string {
	lines := make([]string, 0)
	lines = append(lines, "    Runners: ")
	for _, s := range g.runners.Summary() {
		lines = append(lines, "      "+s)
	}
	lines = append(lines, "    Destination: ")
	lines = append(lines, g.destination.Summary()...)

	if sum := g.internalDests.Summary(); len(sum) > 0 {
		lines = append(lines, "    Internal Destination: ")
		lines = append(lines, sum...)
	}

	return lines
}

// State returns state lines for this group's runners, destination, and any
// internal destinations held by runners.
func (g *group[SE, DE]) State() []string {
	lines := make([]string, 0)
	lines = append(lines, g.runners.State()...)
	lines = append(lines, g.destination.StateString()...)
	lines = append(lines, g.internalDests.StateString()...)

	return lines
}

// Close closes this group's runners, destination, and internal destinations,
// collecting all errors.
func (g *group[SE, DE]) Close() []error {
	var errs []error

	if err := g.runners.Close(); err != nil {
		errs = append(errs, fmt.Errorf("runners.Close: %w", err))
	}

	if err := g.destination.Close(); err != nil {
		errs = append(errs, fmt.Errorf("destination.Close: %w", err))
	}

	if err := g.internalDests.Close(); err != nil {
		errs = append(errs, fmt.Errorf("internal destination.Close: %w", err))
	}

	return errs
}

// OutputForward fans in this group's runners' outputs and forwards them to
// the destination. It blocks until all runners' OutChan are closed and all
// outputs are forwarded.
//
// Concurrency model:
//   - One fan-in goroutine per runner (bounded by runners.Len())
//   - A single forwarder goroutine ranges the merged channel and calls
//     destination.Receive serially (destination needs no thread safety)
//
// Timing constraints:
//   - Prerequisite: Finish has closed all runner.OutChan channels
//   - fan-in goroutines: range OutChan exits → fanIn.Wait() → close(merged)
//   - Forwarder: range merged exits → destination.Done()
func (g *group[SE, DE]) OutputForward(ctx context.Context) error {
	merged := make(chan []DE)
	var fanIn sync.WaitGroup

	for _, r := range g.runners.All() {
		fanIn.Add(1)
		go func(r exec.Runner[SE, DE]) {
			defer fanIn.Done()
			for out := range r.OutChan() {
				select {
				case <-ctx.Done():
					return
				case merged <- out:
				}
			}
		}(r)
	}
	go func() { fanIn.Wait(); close(merged) }()

	for out := range merged {
		if err := g.destination.Receive(out); err != nil {
			return err
		}
	}
	g.destination.Done()

	return nil
}
