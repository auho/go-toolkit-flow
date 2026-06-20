package flow

import (
	"context"
	"fmt"
	"sync"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
)

// groups is a collection of group that encapsulates batch operations.
// It is the flow-internal analogue of exec.Runners for []Runner:
// all lifecycle and data-forwarding logic that iterates over groups lives here,
// keeping flow.go focused on top-level orchestration only.
//
// Concurrency note: the current implementation iterates sequentially.
// Future versions may use errgroup for concurrent execution without
// changing call sites in flow.go.
type groups[SE, DE storage.Entry] []group[SE, DE]

func newGroups[SE, DE storage.Entry]() *groups[SE, DE] {
	gs := make(groups[SE, DE], 0)
	return &gs
}

func (gs *groups[SE, DE]) Add(g group[SE, DE]) {
	*gs = append(*gs, g)
}

func (gs *groups[SE, DE]) Len() int {
	return len(*gs)
}

// TotalRunners returns the count of all runners across all groups.
func (gs *groups[SE, DE]) TotalRunners() int {
	n := 0
	for _, g := range *gs {
		n += g.runners.Len()
	}

	return n
}

// Prepare prepares all groups' runners and destinations.
func (gs *groups[SE, DE]) Prepare(ctx context.Context) error {
	for _, g := range *gs {
		if err := g.Prepare(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Start launches the groups' processing pipeline: first all runners'
// worker goroutines, then all destinations' accept signal.
func (gs *groups[SE, DE]) Start() {
	for _, g := range *gs {
		g.runners.Start()
	}

	for _, g := range *gs {
		g.destination.Accept()
	}
}

// Done signals all groups' runners that no more data will be sent.
func (gs *groups[SE, DE]) Done() {
	for _, g := range *gs {
		g.runners.Done()
	}
}

// Receive fans out items to all runners across all groups.
// When there are multiple runners, copyFn is called to create per-runner
// copies of the items slice, avoiding data races on shared data.
func (gs *groups[SE, DE]) Receive(items []SE, copyFn func([]SE) []SE) {
	needCopy := gs.TotalRunners() > 1

	if needCopy {
		for _, g := range *gs {
			for _, r := range g.runners.All() {
				newItems := copyFn(items)
				r.Receive(newItems)
			}
		}
	} else {
		for _, g := range *gs {
			g.runners.Receive(items)
		}
	}
}

// Finish waits for all groups' runners to complete processing.
// Internally, each group's runners.Finish waits for all runner goroutines
// to exit and then closes each runner's OutChan.
func (gs *groups[SE, DE]) Finish() error {
	for _, g := range *gs {
		if err := g.runners.Finish(); err != nil {
			return fmt.Errorf("runners.Finish: %w", err)
		}
	}

	return nil
}

// OutputForward fans in each group's runner outputs and forwards them
// to the group's destination.
//
// Concurrency model:
//   - Each group runs in its own goroutine (total = len(groups), bounded)
//   - Within each group: one fan-in goroutine per runner
//   - Groups share no state; destinations need no thread safety (each is called
//     serially by one group's single fan-out goroutine)
//
// Timing constraints:
//   - Prerequisite: Finish has closed all runner.OutChan channels
//   - fan-in goroutines: range OutChan exits → close(merged)
//   - Group goroutine: range merged exits → destination.Done()
func (gs *groups[SE, DE]) OutputForward(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, gs.Len())

	for _, g := range *gs {
		wg.Add(1)
		go func(g group[SE, DE]) {
			defer wg.Done()

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
					errCh <- err
					return
				}
			}
			g.destination.Done()
		}(g)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	return nil
}

// DestinationFinish finalizes persistence for all groups' destinations.
// Called after all data has been forwarded and Done.
func (gs *groups[SE, DE]) DestinationFinish() error {
	for _, g := range *gs {
		if err := g.destination.Finish(); err != nil {
			return fmt.Errorf("destination.Finish: %w", err)
		}
	}

	return nil
}

// Close closes all groups' runners and destinations, collecting all errors.
// Returns a slice of errors (one per failed close operation); if no errors,
// returns nil. Callers are responsible for logging each error.
// Future versions may execute closes concurrently via errgroup.
func (gs *groups[SE, DE]) Close() []error {
	var errs []error

	for _, g := range *gs {
		errs = append(errs, g.Close()...)
	}

	return errs
}

// Summary returns summary lines for all groups, including runners and destinations.
func (gs *groups[SE, DE]) Summary() []string {
	lines := make([]string, 0)
	lines = append(lines, "Groups: ")

	for i, g := range *gs {
		lines = append(lines, fmt.Sprintf("  Group %d:", i+1))
		lines = append(lines, g.Summary()...)
	}

	return lines
}

// State returns state lines for all groups' runners and destinations.
func (gs *groups[SE, DE]) State() []string {
	lines := make([]string, 0)

	for _, g := range *gs {
		lines = append(lines, g.State()...)
	}

	return lines
}

// Output returns output lines from all groups' runners.
func (gs *groups[SE, DE]) Output() []string {
	lines := make([]string, 0)

	for _, g := range *gs {
		lines = append(lines, g.runners.Output()...)
	}

	return lines
}
