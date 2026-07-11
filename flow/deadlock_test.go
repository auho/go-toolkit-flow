package flow

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/auho/go-toolkit-flow/v3/exec"
	produceritem "github.com/auho/go-toolkit-flow/v3/exec/producer/item"
	"github.com/auho/go-toolkit-flow/v3/storage"
)

// This file tests deadlock and resource-leak scenarios caused by context
// misalignment and Finish ordering in flow.run().
//
// Context hierarchy (flow.go:136-167):
//   - rootCtx: spans full lifecycle (Prepare → Close), cancelled by defer rootCancel()
//   - asyncCtx: derived from rootCtx via errgroup.WithContext, covers only Phase 3,
//     cancelled when g.Wait() returns (fail-fast)
//
// Context misalignment: source.Prepare(rootCtx) and runner.Prepare(rootCtx) bind
// scanCtx/startCtx to rootCtx, not asyncCtx. When a Phase 3 goroutine fails,
// asyncCtx is cancelled but source/runner contexts don't respond, causing source
// scan goroutine to block on itemsChan <- items, source.Finish() to block on
// scanGroup.Wait(), and g.Wait() to deadlock.
//
// Finish ordering: runners.Finish() collects all errors via errors.Join so
// every runner's outChan is closed even when one fails.
//
// All tests must complete within the timeout; a deadlock manifests as a
// timeout failure.

const deadlockTimeout = 3 * time.Second

// runFlowWithTimeout runs RunFlow in a goroutine and fails the test if it
// does not return within timeout. This catches deadlocks where g.Wait()
// blocks forever due to ctx misalignment or Finish ordering.
func runFlowWithTimeout(t *testing.T, opts []Option[storage.MapEntry, storage.MapEntry]) error {
	t.Helper()

	done := make(chan error, 1)
	go func() { done <- RunFlow(opts...) }()

	select {
	case err := <-done:
		return err
	case <-time.After(deadlockTimeout):
		t.Fatalf("RunFlow deadlocked after %v", deadlockTimeout)
		return nil
	}
}

// buildOpts builds flow options from a source, a single runner (faultProducerItem),
// and a destination.
func buildOpts(
	src *faultSource,
	proc *faultProducerItem,
	dest storage.Destination[storage.MapEntry],
) []Option[storage.MapEntry, storage.MapEntry] {
	return []Option[storage.MapEntry, storage.MapEntry]{
		WithSource[storage.MapEntry, storage.MapEntry](src),
		WithGroup[storage.MapEntry, storage.MapEntry](
			[]exec.Runner[storage.MapEntry, storage.MapEntry]{
				produceritem.NewRunner[storage.MapEntry, storage.MapEntry](proc),
			},
			dest,
		),
	}
}

// buildOptsMultiRunner builds flow options with multiple runners in one group.
func buildOptsMultiRunner(
	src *faultSource,
	procs []*faultProducerItem,
	dest storage.Destination[storage.MapEntry],
) []Option[storage.MapEntry, storage.MapEntry] {
	runners := make([]exec.Runner[storage.MapEntry, storage.MapEntry], 0, len(procs))
	for _, p := range procs {
		runners = append(runners, produceritem.NewRunner[storage.MapEntry, storage.MapEntry](p))
	}

	return []Option[storage.MapEntry, storage.MapEntry]{
		WithSource[storage.MapEntry, storage.MapEntry](src),
		WithGroup[storage.MapEntry, storage.MapEntry](runners, dest),
	}
}

// buildOptsMultiGroup builds flow options with multiple groups.
func buildOptsMultiGroup(
	src *faultSource,
	procs []*faultProducerItem,
	dests []storage.Destination[storage.MapEntry],
) []Option[storage.MapEntry, storage.MapEntry] {
	opts := []Option[storage.MapEntry, storage.MapEntry]{
		WithSource[storage.MapEntry, storage.MapEntry](src),
	}
	for i, p := range procs {
		opts = append(opts, WithGroup[storage.MapEntry, storage.MapEntry](
			[]exec.Runner[storage.MapEntry, storage.MapEntry]{
				produceritem.NewRunner[storage.MapEntry, storage.MapEntry](p),
			},
			dests[i],
		))
	}
	return opts
}

// errorContains checks if err's message contains substr.
func errorContains(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("expected error containing %q, got %q", substr, err.Error())
	}
}

// =====================================================================================
// T2: runner worker failure (E3 startGroup) - context deadlock
// =====================================================================================

// TestDeadlock_WorkerExecError verifies that a worker Exec error does not
// deadlock the flow. Without the two-layer context hierarchy: asyncCtx cancels
// -> transport exits -> source scan goroutine blocks on itemsChan -> source.Finish
// blocks -> g.Wait() deadlocks.
func TestDeadlock_WorkerExecError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:     1000,
		pageSize:  10,
		scanBlock: make(chan struct{}),
	})
	proc := newFaultProducerItem(faultProducerConfig{
		execAt:  1,
		execErr: errExec,
	})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))

	// Should return an error containing "executor.Exec" or "exec" before timeout.
	errorContains(t, err, "exec")
}

// TestDeadlock_WorkerExecError_MultiWorker tests the same scenario with
// multiple worker goroutines (concurrency > 1).
func TestDeadlock_WorkerExecError_MultiWorker(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:     1000,
		pageSize:  10,
		scanBlock: make(chan struct{}),
	})
	proc := newFaultProducerItem(faultProducerConfig{
		execAt:      1,
		execErr:     errExec,
		concurrency: 4,
	})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "exec")
}

// =====================================================================================
// T3: processor.AfterRun failure - deadlock + resource leak
// =====================================================================================

// TestDeadlock_AfterRunError verifies that an AfterRun error does not deadlock
// or leak. runners.Finish() collects all errors via errors.Join, ensuring
// every runner's outChan is closed even when one fails. A first-error-return
// would skip subsequent outChan closes, causing OutputForward to block on
// un-closed outChan and g.Wait() to deadlock.
//
// Note: unlike T2/T4/T6, this test does NOT use scanBlock. The AfterRun error
// only fires after workers complete, which requires the source to finish
// normally. The deadlock arises from unclosed outChan, not source blocking.
func TestDeadlock_AfterRunError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    100,
		pageSize: 10,
	})
	proc1 := newFaultProducerItem(faultProducerConfig{
		afterRunErr: errAfterRun,
	})
	proc2 := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOptsMultiRunner(src, []*faultProducerItem{proc1, proc2}, dest))
	errorContains(t, err, "AfterRun")

	// Leak assertion: both runners' AfterRun should be called.
	if proc1.afterRunCalled.Load() != 1 {
		t.Errorf("proc1.AfterRun called %d times, want 1", proc1.afterRunCalled.Load())
	}
	if proc2.afterRunCalled.Load() != 1 {
		t.Errorf("proc2.AfterRun called %d times, want 1 (leak: skipped by sequential Finish)", proc2.afterRunCalled.Load())
	}
}

// =====================================================================================
// T4: destination.Receive failure - context deadlock variant
// =====================================================================================

// TestDeadlock_DestinationReceiveError verifies that a destination.Receive error
// does not deadlock. The two-layer context hierarchy ensures that when
// OutputForward returns an error and asyncCtx cancels, the transport
// goroutine exits via ctx.Done() without blocking the source.
func TestDeadlock_DestinationReceiveError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:     1000,
		pageSize:  10,
		scanBlock: make(chan struct{}),
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{
		receiveErr: errDestReceive,
	})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Receive")
}

// =====================================================================================
// T6: multi-group destGroup failure - context deadlock variant
// =====================================================================================

// TestDeadlock_MultiGroupDestError verifies that a destination.Receive error in
// one of multiple groups does not deadlock. Source completes normally (no
// scanBlock); the dest error is surfaced via destGroup's errgroup.
func TestDeadlock_MultiGroupDestError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    100,
		pageSize: 10,
	})
	proc1 := newFaultProducerItem(faultProducerConfig{})
	proc2 := newFaultProducerItem(faultProducerConfig{})
	dest1 := newFaultDestination(faultDestinationConfig{})
	dest2 := newFaultDestination(faultDestinationConfig{
		receiveErr: errDestReceive,
	})

	err := runFlowWithTimeout(t, buildOptsMultiGroup(src, []*faultProducerItem{proc1, proc2}, []storage.Destination[storage.MapEntry]{dest1, dest2}))
	errorContains(t, err, "Receive")
}

// =====================================================================================
// T7: fan-in goroutine ctx cancellation - OutputForward blocked on OutChan
// =====================================================================================

// TestDeadlock_FanInCtxCancellation verifies that fan-in goroutines exit
// promptly when ctx is cancelled, even if OutChan is open but not producing
// data. Fan-in goroutines use select with ctx.Done() so they can exit
// immediately when cancelled. A plain `for out := range r.OutChan()` would
// block on receive, preventing merged from closing and causing a deadlock
// when a destination error cancels asyncCtx.
//
// Scenario:
//   - Source with scanBlock: produces 1 batch then blocks (OutChan stays open)
//   - Destination with receiveErr: fails on first Receive
//   - Fan-in goroutines use select with ctx.Done() so they exit when
//     cancelled, allowing merged to close and preventing deadlock.
func TestDeadlock_FanInCtxCancellation(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:     1000,
		pageSize:  10,
		scanBlock: make(chan struct{}),
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{
		receiveErr: errDestReceive,
	})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Receive")
}

// =====================================================================================
// T1: source scan failure (E2 scanGroup) — error path, no deadlock expected
// =====================================================================================

// TestDeadlock_SourceScanError verifies that a source scan error is properly
// surfaced. scanErr is injected during scan; the scan goroutine produces 1 batch
// then exits, so source.Finish returns quickly. With only 1 batch, the pipeline
// may complete before asyncCtx cancels (race). With the two-layer context
// hierarchy, this test passes deterministically because workers exit via
// startCtx.Done() when asyncCtx cancels.
func TestDeadlock_SourceScanError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:   100,
		pageSize: 10,
		scanErr: errSourceScan,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Scan")
}

// =====================================================================================
// T5: source.Finish failure (E1 main g) - error path, no deadlock expected
// =====================================================================================

// TestDeadlock_SourceFinishError verifies that a source.Finish error does not
// deadlock. Without the two-layer context hierarchy: source.Finish returns error
// -> asyncCtx cancels -> fanIn goroutines exit early via ctx.Done() -> workers
// block on outChan<- (no reader, startCtx=rootCtx not cancelled) -> runner.Finish
// blocks on startGroup.Wait() -> g.Wait() deadlocks.
func TestDeadlock_SourceFinishError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:     100,
		pageSize:  10,
		finishErr: errSourceFinish,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Finish")
}

// =====================================================================================
// Lifecycle error path tests — verify proper error surfacing and resource cleanup
// =====================================================================================

// TestError_SourcePrepareError verifies source.Prepare failure in Phase 1.
func TestError_SourcePrepareError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		prepareErr: errSourcePrepare,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Prepare")
}

// TestError_SourceCloseError verifies source.Close failure is collected (not fatal).
func TestError_SourceCloseError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
		closeErr: errSourceClose,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	// Close errors are logged, not returned. RunFlow should succeed.
	if err != nil {
		t.Errorf("expected nil error for source.Close failure, got %v", err)
	}
	if src.closeCalled.Load() != 1 {
		t.Errorf("source.Close called %d times, want 1", src.closeCalled.Load())
	}
}

// TestError_DestPrepareError verifies destination.Prepare failure in Phase 1.
func TestError_DestPrepareError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{
		prepareErr: errDestPrepare,
	})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Prepare")
}

// TestError_DestFinishError verifies destination.Finish failure in Phase 4.
func TestError_DestFinishError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{
		finishErr: errDestFinish,
	})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Finish")
}

// TestError_DestCloseError verifies destination.Close failure is collected.
func TestError_DestCloseError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{
		closeErr: errDestClose,
	})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	if err != nil {
		t.Errorf("expected nil error for dest.Close failure, got %v", err)
	}
	if dest.closeCalled.Load() != 1 {
		t.Errorf("dest.Close called %d times, want 1", dest.closeCalled.Load())
	}
}

// TestError_ProcPrepareError verifies processor.Prepare failure in Phase 1.
func TestError_ProcPrepareError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{
		prepareErr: errProcPrepare,
	})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "Prepare")
}

// TestError_ProcBeforeRunError verifies processor.BeforeRun failure in Phase 1.
func TestError_ProcBeforeRunError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{
		beforeRunErr: errBeforeRun,
	})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	errorContains(t, err, "BeforeRun")
}

// TestError_ProcCloseError verifies processor.Close failure is collected.
func TestError_ProcCloseError(t *testing.T) {
	src := newFaultSource(faultSourceConfig{
		total:    10,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{
		closeErr: errProcClose,
	})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	if err != nil {
		t.Errorf("expected nil error for processor.Close failure, got %v", err)
	}
	if proc.closeCalled.Load() != 1 {
		t.Errorf("processor.Close called %d times, want 1", proc.closeCalled.Load())
	}
}

// =====================================================================================
// Happy path baseline — ensures fault mocks work correctly without faults
// =====================================================================================

// TestFaultMock_HappyPath verifies that the fault mocks produce a correct flow
// when no errors are injected. This is a sanity check for the mock infrastructure.
func TestFaultMock_HappyPath(t *testing.T) {
	total := int64(100)
	src := newFaultSource(faultSourceConfig{
		total:    total,
		pageSize: 10,
	})
	proc := newFaultProducerItem(faultProducerConfig{})
	dest := newFaultDestination(faultDestinationConfig{})

	err := runFlowWithTimeout(t, buildOpts(src, proc, dest))
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Lifecycle call counts
	if src.prepareCalled.Load() != 1 {
		t.Errorf("source.Prepare called %d times, want 1", src.prepareCalled.Load())
	}
	if src.scanCalled.Load() != 1 {
		t.Errorf("source.Scan called %d times, want 1", src.scanCalled.Load())
	}
	if src.finishCalled.Load() != 1 {
		t.Errorf("source.Finish called %d times, want 1", src.finishCalled.Load())
	}
	if src.closeCalled.Load() != 1 {
		t.Errorf("source.Close called %d times, want 1", src.closeCalled.Load())
	}
	// destination.Receive is called once per output batch (10 batches, not 100 items)
	wantReceive := total / 10
	if dest.receiveCalled.Load() != wantReceive {
		t.Errorf("dest.Receive called %d times, want %d", dest.receiveCalled.Load(), wantReceive)
	}
	if dest.doneCalled.Load() < 1 {
		t.Errorf("dest.Done called %d times, want >= 1", dest.doneCalled.Load())
	}
	if dest.finishCalled.Load() != 1 {
		t.Errorf("dest.Finish called %d times, want 1", dest.finishCalled.Load())
	}
	if dest.closeCalled.Load() != 1 {
		t.Errorf("dest.Close called %d times, want 1", dest.closeCalled.Load())
	}
	if proc.afterRunCalled.Load() != 1 {
		t.Errorf("proc.AfterRun called %d times, want 1", proc.afterRunCalled.Load())
	}
}

// =====================================================================================
// Wrap-error helper for assertions
// =====================================================================================

// ensure errors package is used (for future sentinel error comparisons)
var _ = errors.Is
