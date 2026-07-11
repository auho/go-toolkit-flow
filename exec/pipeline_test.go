package exec

import (
	"context"
	"testing"
	"time"

	testutilprocessor "github.com/auho/go-toolkit-flow/v3/internal/testutil/processor"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// transformExecutor transforms []SE to []DE by applying a function.
type transformExecutor[SE, DE storage.Entry] struct {
	fn func(SE) DE
}

func (e *transformExecutor[SE, DE]) Exec(items []SE) ([]DE, int64, int64, error) {
	out := make([]DE, len(items))
	for i, item := range items {
		out[i] = e.fn(item)
	}
	return out, int64(len(items)), int64(len(items)), nil
}

// simpleProc is a minimal processor for multi-stage tests.
type simpleProc struct {
	testutilprocessor.TestProcessor
}

func (p *simpleProc) Concurrency() int { return 1 }
func (p *simpleProc) Summary() string  { return "simpleProc" }

// makeStageRunner creates a Runner that transforms items via fn.
func makeStageRunner[SE, DE storage.Entry](fn func(SE) DE) Runner[SE, DE] {
	return NewRunner[SE, DE](&transformExecutor[SE, DE]{fn: fn}, &simpleProc{})
}

func TestPipeline_DataFlow(t *testing.T) {
	// Stage 1: double id
	// Stage 2: add 100 to id
	// Input id=1 -> 2 -> 102
	// Input id=5 -> 10 -> 110
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id * 2}
	})
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id + 100}
	})

	runner := Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2).Build()

	ctx := context.Background()
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	var collected []storage.MapEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range runner.OutChan() {
			collected = append(collected, batch...)
		}
	}()

	runner.Receive([]storage.MapEntry{{"id": 1}, {"id": 5}})
	runner.Done()

	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
	defer runner.Close()

	if len(collected) != 2 {
		t.Fatalf("expected 2 items, got %d", len(collected))
	}

	results := make(map[int]bool)
	for _, item := range collected {
		id, _ := item["id"].(int)
		results[id] = true
	}
	if !results[102] || !results[110] {
		t.Fatalf("expected ids 102 and 110, got %v", results)
	}
}

func TestPipeline_SingleStage(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id + 1}
	})

	runner := Stage(NewPipeline[storage.MapEntry](), r1).Build()

	ctx := context.Background()
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	var collected []storage.MapEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range runner.OutChan() {
			collected = append(collected, batch...)
		}
	}()

	runner.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}, {"id": 3}})
	runner.Done()

	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
	defer runner.Close()

	if len(collected) != 3 {
		t.Fatalf("expected 3 items, got %d", len(collected))
	}
}

func TestPipeline_ThreeStages(t *testing.T) {
	// Stage 1: +1
	// Stage 2: *2
	// Stage 3: +100
	// Input id=1 -> 2 -> 4 -> 104
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id + 1}
	})
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id * 2}
	})
	r3 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		id, _ := e["id"].(int)
		return storage.MapEntry{"id": id + 100}
	})

	runner := Stage(Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2), r3).Build()

	ctx := context.Background()
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	var collected []storage.MapEntry
	done := make(chan struct{})
	go func() {
		defer close(done)
		for batch := range runner.OutChan() {
			collected = append(collected, batch...)
		}
	}()

	runner.Receive([]storage.MapEntry{{"id": 1}})
	runner.Done()

	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
	defer runner.Close()

	if len(collected) != 1 {
		t.Fatalf("expected 1 item, got %d", len(collected))
	}
	id, _ := collected[0]["id"].(int)
	if id != 104 {
		t.Fatalf("expected id=104, got %d", id)
	}
}

func TestPipeline_CascadingShutdown(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})

	runner := Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2).Build()

	ctx := context.Background()
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	received := make(chan struct{})
	go func() {
		for range runner.OutChan() {
		}
		close(received)
	}()

	runner.Receive([]storage.MapEntry{{"id": 1}})
	runner.Done()

	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	<-received
	defer runner.Close()
}

// lifecycleDest is a mock Destination that records lifecycle method calls in order.
type lifecycleDest struct {
	calls []string
}

func (d *lifecycleDest) Prepare(context.Context) error                { d.calls = append(d.calls, "prepare"); return nil }
func (d *lifecycleDest) Accept()                                      { d.calls = append(d.calls, "accept") }
func (d *lifecycleDest) Receive([]storage.MapEntry) error             { return nil }
func (d *lifecycleDest) Done()                                        { d.calls = append(d.calls, "done") }
func (d *lifecycleDest) Finish() error                                { d.calls = append(d.calls, "finish"); return nil }
func (d *lifecycleDest) Close() error                                 { d.calls = append(d.calls, "close"); return nil }
func (d *lifecycleDest) Copy(items []storage.MapEntry) []storage.MapEntry { return items }
func (d *lifecycleDest) Summary() []string                            { return nil }
func (d *lifecycleDest) State() storage.State                         { return storage.NewSnapshot() }
func (d *lifecycleDest) StateString() []string                        { return nil }

// holderProc is a processor that holds an internal destination via
// storage.DestinationHolder, so the pipeline stage manages its lifecycle.
type holderProc struct {
	testutilprocessor.TestProcessor
	dest *lifecycleDest
}

func (p *holderProc) Concurrency() int { return 1 }
func (p *holderProc) Summary() string  { return "holderProc" }
func (p *holderProc) HeldDestinations() ([]storage.Destination[storage.MapEntry], error) {
	return []storage.Destination[storage.MapEntry]{p.dest}, nil
}

// TestPipeline_InternalDestinationLifecycle verifies that internal destinations
// held by a stage's processor receive lifecycle calls in the correct order:
// prepare -> accept -> done -> finish -> close.
func TestPipeline_InternalDestinationLifecycle(t *testing.T) {
	dest := &lifecycleDest{}

	// Stage 1: processor holds an internal destination
	r1 := NewRunner[storage.MapEntry, storage.MapEntry](
		&transformExecutor[storage.MapEntry, storage.MapEntry]{fn: func(e storage.MapEntry) storage.MapEntry {
			return storage.MapEntry{"id": e["id"]}
		}},
		&holderProc{dest: dest},
	)
	// Stage 2: simple pass-through
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})

	runner := Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2).Build()

	ctx := context.Background()
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range runner.OutChan() {
		}
	}()

	runner.Receive([]storage.MapEntry{{"id": 1}})
	runner.Done()

	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done

	if err := runner.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify lifecycle order: prepare -> accept -> done -> finish -> close
	expected := []string{"prepare", "accept", "done", "finish", "close"}
	if len(dest.calls) != len(expected) {
		t.Fatalf("expected %d lifecycle calls, got %d: %v", len(expected), len(dest.calls), dest.calls)
	}
	for i, want := range expected {
		if dest.calls[i] != want {
			t.Fatalf("lifecycle call[%d]: expected %q, got %q (full: %v)", i, want, dest.calls[i], dest.calls)
		}
	}
}

func TestPipeline_Destinations_ReturnsNil(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return e
	})

	runner := Stage(NewPipeline[storage.MapEntry](), r1).Build()

	if dests := runner.Destinations(); dests != nil {
		t.Fatalf("expected nil, got %v", dests)
	}
}

func TestPipeline_Summary(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return e
	})

	runner := Stage(NewPipeline[storage.MapEntry](), r1).Build()

	ctx := context.Background()
	_ = runner.Prepare(ctx, ctx)

	summary := runner.Summary()
	found := false
	for _, line := range summary {
		if line == "  Stage 0:" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 'Stage 0:' in summary, got %v", summary)
	}

	defer runner.Close()
}

func TestPipelineBuilder_TypeInference(t *testing.T) {
	// Verify that type parameters are inferred correctly at compile time
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return e
	})

	// This should compile without explicit type parameters
	runner := Stage(NewPipeline[storage.MapEntry](), r1).Build()

	// Verify it implements Runner
	var _ Runner[storage.MapEntry, storage.MapEntry] = runner
}

// TestPipeline_BridgeCtxCancellation verifies that bridge goroutines in the
// Stage function's outChan closure and pipelineRunner.OutChan() exit promptly
// when ctx is cancelled, even if the runner's OutChan is still open.
//
// Bridge goroutines use select with ctx.Done() so they can exit immediately
// when ctx is cancelled. A plain `for items := range src` would block on
// receive and stay blocked until Finish() closes the runner's OutChan.
func TestPipeline_BridgeCtxCancellation(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})

	runner := Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2).Build()

	ctx, cancel := context.WithCancel(context.Background())
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	// Get OutChan reference - starts the bridge goroutine
	outCh := runner.OutChan()

	// Cancel ctx - bridge goroutines should exit and close OutChan
	cancel()

	// OutChan should close within timeout (bridge goroutine exited via ctx.Done)
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()

	for {
		select {
		case _, ok := <-outCh:
			if !ok {
				// OutChan closed - bridge goroutines exited via ctx.Done()
				goto done
			}
		case <-timer.C:
			t.Fatal("OutChan did not close after ctx cancellation (bridge goroutine leak)")
		}
	}

done:
	runner.Done()
	if err := runner.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	defer runner.Close()
}

// TestPipeline_FinishNoDeadlockOnCtxCancel verifies that Finish() does not
// deadlock when ctx has been cancelled before Finish is called.
func TestPipeline_FinishNoDeadlockOnCtxCancel(t *testing.T) {
	r1 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})
	r2 := makeStageRunner(func(e storage.MapEntry) storage.MapEntry {
		return storage.MapEntry{"id": e["id"]}
	})

	runner := Stage(Stage(NewPipeline[storage.MapEntry](), r1), r2).Build()

	ctx, cancel := context.WithCancel(context.Background())
	if err := runner.Prepare(ctx, ctx); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runner.Start()

	// Send data and cancel ctx immediately (simulating error in another part)
	runner.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	runner.Done()
	cancel()

	// Finish should return promptly, not deadlock
	done := make(chan error, 1)
	go func() { done <- runner.Finish() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Finish returned error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Finish deadlocked after ctx cancellation")
	}

	defer runner.Close()
}
