// Package flow is the top-level orchestration layer.
// It wires together Source → groups (runners + Destination) and manages the
// full lifecycle: Prepare → Start → (async: transport | finish | output) → Finish → Close.
package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit/console/output"
	"github.com/auho/go-toolkit/time/timing"
	"golang.org/x/sync/errgroup"
)

// Option configures a flow.
type Option[SE, DE storage.Entry] func(*flow[SE, DE])

// WithSource sets the data source for the flow.
func WithSource[SE, DE storage.Entry](se storage.Source[SE]) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.source = se
	}
}

// WithStateInterval sets the state refresh interval.
func WithStateInterval[SE, DE storage.Entry](d time.Duration) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		f.stateInterval = d
	}
}

// WithGroup registers a group of runners bound to one or more destinations.
//   - 0 dests: destination defaults to NoopDestination (consumer path, no data produced)
//   - 1 dest:   single destination
//   - N dests:  wrapped as MultiDestination (fan-out to all destinations)
//
// Each group runs independently: runners' outputs are fan-in merged within the group,
// then forwarded to the group's destination(s). Groups execute concurrently.
func WithGroup[SE, DE storage.Entry](
	runners []exec.Runner[SE, DE],
	dests ...storage.Destination[DE],
) Option[SE, DE] {
	return func(f *flow[SE, DE]) {
		rs := exec.NewRunners[SE, DE]()
		rs.Add(runners...)

		var dest storage.Destination[DE]
		switch len(dests) {
		case 0:
			dest = storage.NoopDestination[DE]{}
		case 1:
			dest = dests[0]
		default:
			dest = storage.MultiDestination[DE](dests)
		}

		f.groups.Add(group[SE, DE]{
			runners:     rs,
			destination: dest,
		})
	}
}

// flow holds the source and groups, orchestrating the full lifecycle.
// Data flows: Source → transport(fan-out) → [group1.runners, group2.runners, ...]
//                                         → executor.Exec → runner.OutChan
//                                         → per-group fan-in → group.destination
type flow[SE, DE storage.Entry] struct {
	source        storage.Source[SE]
	groups        *groups[SE, DE]
	refreshOutput *output.Refresh
	stateInterval time.Duration
}

// RunFlow is the entry point. It validates options, executes the full lifecycle
// (check → run → close), and returns any error encountered.
func RunFlow[SE, DE storage.Entry](opts ...Option[SE, DE]) error {
	d := timing.NewDuration()
	d.Start()

	f := &flow[SE, DE]{
		groups: NewGroups[SE, DE](),
	}
	for _, o := range opts {
		o(f)
	}

	err := f.check()
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}

	err = f.run()
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}

	d.StringStartToStop()

	return nil
}

func (f *flow[SE, DE]) check() error {
	if f.source == nil {
		return errors.New("source not found")
	}

	if f.groups.Len() == 0 {
		return errors.New("group not found")
	}

	return nil
}

// run executes the data processing lifecycle.
//
// Data flow:
//   Source → transport(fan-out) → [group1.runners, group2.runners, ...]
//                                    ↓ executor.Exec
//                              [runner.OutChan, ...]
//                                    ↓ per-group fan-in
//                              group.destination.Receive
//                                    ↓ (MultiDestination fan-out)
//                              [sub-dest1, sub-dest2, ...]
//
// Lifecycle phases:
//   1. Prepare: source.Prepare → groups.Prepare (runners + destination)
//   2. Start:   source.Scan → groups.Start (runners) → groups.Accept (destination)
//   3. Async:   errgroup { source.Finish, transport, groups.Finish, groups.OutputForward }
//   4. Finish:  groups.DestinationFinish
//   5. Close (deferred): source.Close → groups.Close (runners + destination)
func (f *flow[SE, DE]) run() error {
	defer f.close()

	f.refreshOutput = output.NewRefresh(
		output.WithInterval(f.stateInterval),
		output.WithContent(func() ([]string, error) {
			return f.state(), nil
		}),
	)

	// errgroup with cancel for coordinating async goroutines
	g, ctx := errgroup.WithContext(context.Background())

	// === Phase 1: Prepare ===
	// All Prepare calls are synchronous so that errors are surfaced before any goroutines start.
	err := f.source.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("source.Prepare: %w", err)
	}

	err = f.groups.Prepare(ctx)
	if err != nil {
		return fmt.Errorf("groups.Prepare: %w", err)
	}

	f.summary()

	// === Phase 2: Start ===
	// Non-blocking: Scan/Start/Accept launch producer goroutines internally.
	f.source.Scan()
	f.groups.Start()
	f.groups.Accept()

	f.refreshOutput.Start()

	// === Phase 3: Async concurrent processing ===
	g.Go(func() error {
		if err := f.source.Finish(); err != nil {
			return fmt.Errorf("source.Finish: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		f.transport(ctx)
		return nil
	})

	g.Go(func() error {
		if err := f.groups.Finish(); err != nil {
			return fmt.Errorf("groups.Finish: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := f.groups.OutputForward(ctx); err != nil {
			return fmt.Errorf("groups.OutputForward: %w", err)
		}

		return nil
	})

	// Wait for all async goroutines to complete.
	if err = g.Wait(); err != nil {
		return err
	}

	// === Phase 4: Finish ===
	// Destination persistence is finalized after all data has been forwarded and Done.
	err = f.groups.DestinationFinish()
	if err != nil {
		return fmt.Errorf("groups.DestinationFinish: %w", err)
	}

	return nil
}

// transport reads from the source channel and fans out data to all groups' runners.
//
// Concurrency model:
//   - Single goroutine reading from source.ReceiveChan
//   - For each batch: delegates fan-out (with Copy when needed) to groups.Receive
//   - On source channel closed or ctx cancelled: signals all runners via groups.Done()
func (f *flow[SE, DE]) transport(ctx context.Context) {
	for {
		select {
		case items, ok := <-f.source.ReceiveChan():
			if !ok {
				f.groups.Done()
				return
			}

			f.groups.Receive(items, f.source.Copy)

		case <-ctx.Done():
			f.groups.Done()
			return
		}
	}
}

// close releases all resources in reverse preparation order.
func (f *flow[SE, DE]) close() {
	defer func() {
		f.refreshOutput.Stop()
		f.runnersOutput()
	}()

	if err := f.source.Close(); err != nil {
		f.refreshOutput.PrintNext(fmt.Errorf("source.Close: %w", err).Error())
	}

	for _, err := range f.groups.Close() {
		f.refreshOutput.PrintNext(err.Error())
	}
}

func (f *flow[SE, DE]) summary() {
	lines := f.source.Summary()
	lines = append(lines, f.groups.Summary()...)

	for _, s := range lines {
		fmt.Println(s)
	}
	fmt.Println("")
}

func (f *flow[SE, DE]) state() []string {
	lines := make([]string, 0)
	lines = append(lines, f.source.State()...)
	lines = append(lines, f.groups.State()...)

	return lines
}

func (f *flow[SE, DE]) runnersOutput() {
	fmt.Println("\nOutput: ")

	for _, s := range f.groups.Output() {
		fmt.Println(s)
	}

	fmt.Println()
}