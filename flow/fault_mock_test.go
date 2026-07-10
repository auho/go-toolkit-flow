package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/tool"
	testutilprocessor "github.com/auho/go-toolkit-flow/v3/internal/testutil/processor"
)

// This file defines fault-injecting mocks for deadlock/leak testing.
// All mocks target storage.MapEntry to match the existing mock_flow_test.go
// combination tests.
//
// Design principles:
//   - Every lifecycle method has an atomic call counter for leak assertions.
//   - Each method has an injectable error field (xxxErr).
//   - faultSource.scanCtx derives from the ctx passed to Prepare (rootCtx in
//     flow.run), mirroring real source implementations. This is critical for
//     reproducing context deadlock: scanCtx does NOT respond to asyncCtx cancellation.
//   - faultSource.scanBlock allows the scan goroutine to produce one batch then
//     block, guaranteeing stable deadlock reproduction (no races with scan speed).

// =====================================================================================
// faultSource
// =====================================================================================

var _ storage.Source[storage.MapEntry] = (*faultSource)(nil)

type faultSourceConfig struct {
	total      int64
	pageSize   int64
	prepareErr error
	scanErr    error         // injected during scan; surfaced via Finish
	finishErr  error         // overrides scanErr in Finish
	closeErr   error
	scanBlock  chan struct{} // non-nil: produce 1 batch then block until closed
}

type faultSource struct {
	cfg faultSourceConfig

	scanCtx   context.Context
	itemsChan chan []storage.MapEntry
	scanWg    sync.WaitGroup
	state     *storage.PageSnapshot

	prepareCalled atomic.Int64
	scanCalled    atomic.Int64
	finishCalled  atomic.Int64
	closeCalled   atomic.Int64
}

func newFaultSource(cfg faultSourceConfig) *faultSource {
	if cfg.total <= 0 {
		cfg.total = 100
	}
	if cfg.pageSize <= 0 {
		cfg.pageSize = 10
	}

	s := &faultSource{cfg: cfg}
	s.state = storage.NewPageSnapshot()
	s.state.SetTotal(cfg.total)
	s.state.SetPageSize(cfg.pageSize)
	s.state.SetConcurrency(1)
	s.state.SetTitle("faultSource")
	s.state.MarkAsConfigured()

	return s
}

func (s *faultSource) Prepare(ctx context.Context) error {
	s.prepareCalled.Add(1)
	s.scanCtx = ctx
	s.itemsChan = make(chan []storage.MapEntry, 1)
	s.state.MarkAsPrepare()
	return s.cfg.prepareErr
}

func (s *faultSource) Scan() {
	s.scanCalled.Add(1)
	s.state.MarkAsScanning()
	s.state.DurationStart()

	s.scanWg.Go(func() {
		// Produce one batch first.
		items := make([]storage.MapEntry, s.cfg.pageSize)
		for i := range items {
			items[i] = storage.MapEntry{"id": int64(i + 1)}
		}

		select {
		case s.itemsChan <- items:
		case <-s.scanCtx.Done():
			return
		}

		// If scanErr is set, record it and stop producing (scanErr is
		// surfaced via Finish).
		if s.cfg.scanErr != nil {
			return
		}

		// If scanBlock is set, block here until the channel is closed or
		// scanCtx is cancelled. This keeps the scan goroutine alive after
		// transport exits, reproducing the context deadlock.
		if s.cfg.scanBlock != nil {
			select {
			case <-s.scanCtx.Done():
				return
			case <-s.cfg.scanBlock:
				// released by test
			}
			return
		}

		// Normal mode: produce remaining batches.
		produced := s.cfg.pageSize
		for produced < s.cfg.total {
			remaining := s.cfg.total - produced
			size := s.cfg.pageSize
			if size > remaining {
				size = remaining
			}

			batch := make([]storage.MapEntry, size)
			for i := range batch {
				batch[i] = storage.MapEntry{"id": produced + int64(i) + 1}
			}

			select {
			case s.itemsChan <- batch:
			case <-s.scanCtx.Done():
				return
			}

			produced += size
		}
	})
}

func (s *faultSource) ReceiveChan() <-chan []storage.MapEntry {
	return s.itemsChan
}

func (s *faultSource) Finish() error {
	s.finishCalled.Add(1)
	s.scanWg.Wait()

	close(s.itemsChan)
	s.state.DurationStop()
	s.state.MarkAsFinished()

	if s.cfg.finishErr != nil {
		return s.cfg.finishErr
	}
	return s.cfg.scanErr
}

func (s *faultSource) Close() error {
	s.closeCalled.Add(1)
	return s.cfg.closeErr
}

func (s *faultSource) Summary() []string {
	return []string{fmt.Sprintf("faultSource: total=%d, pageSize=%d", s.cfg.total, s.cfg.pageSize)}
}

func (s *faultSource) State() storage.State {
	return s.state
}

func (s *faultSource) StateString() []string {
	return []string{s.state.Overview()}
}

func (s *faultSource) Copy(items []storage.MapEntry) []storage.MapEntry {
	cp := make([]storage.MapEntry, len(items))
	for i, item := range items {
		newItem := make(storage.MapEntry, len(item))
		for k, v := range item {
			newItem[k] = v
		}
		cp[i] = newItem
	}
	return cp
}

// ReleaseScan unblocks a scanBlock-configured source. Safe to call multiple times.
func (s *faultSource) ReleaseScan() {
	if s.cfg.scanBlock != nil {
		select {
		case <-s.cfg.scanBlock:
			// already closed
		default:
			close(s.cfg.scanBlock)
		}
	}
}

// =====================================================================================
// faultDestination
// =====================================================================================

var _ storage.Destination[storage.MapEntry] = (*faultDestination)(nil)

type faultDestinationConfig struct {
	prepareErr error
	receiveErr error
	finishErr  error
	closeErr   error
}

type faultDestination struct {
	cfg faultDestinationConfig

	isDone    atomic.Bool
	state     *storage.Snapshot
	itemsChan chan []storage.MapEntry
	chanWg    sync.WaitGroup

	prepareCalled  atomic.Int64
	acceptCalled   atomic.Int64
	receiveCalled  atomic.Int64
	doneCalled     atomic.Int64
	finishCalled   atomic.Int64
	closeCalled    atomic.Int64
}

func newFaultDestination(cfg faultDestinationConfig) *faultDestination {
	d := &faultDestination{cfg: cfg}
	d.state = storage.NewSnapshot()
	d.state.SetTitle("faultDestination")
	d.state.MarkAsConfigured()
	return d
}

func (d *faultDestination) Prepare(_ context.Context) error {
	d.prepareCalled.Add(1)
	d.state.MarkAsPrepare()
	return d.cfg.prepareErr
}

func (d *faultDestination) Accept() {
	d.acceptCalled.Add(1)
	d.state.MarkAsAccepted()
	d.state.DurationStart()
	d.itemsChan = make(chan []storage.MapEntry)

	d.chanWg.Go(func() {
		for items := range d.itemsChan {
			d.state.AddAmount(int64(len(items)))
		}
	})
}

func (d *faultDestination) Receive(items []storage.MapEntry) error {
	d.receiveCalled.Add(1)
	if d.cfg.receiveErr != nil {
		return d.cfg.receiveErr
	}
	d.itemsChan <- items
	return nil
}

func (d *faultDestination) Done() {
	d.doneCalled.Add(1)
	if !d.isDone.CompareAndSwap(false, true) {
		return
	}
	d.state.MarkAsDone()
	close(d.itemsChan)
}

func (d *faultDestination) Finish() error {
	d.finishCalled.Add(1)
	d.chanWg.Wait()
	d.state.DurationStop()
	d.state.MarkAsFinished()
	return d.cfg.finishErr
}

func (d *faultDestination) Close() error {
	d.closeCalled.Add(1)
	return d.cfg.closeErr
}

func (d *faultDestination) Copy(items []storage.MapEntry) []storage.MapEntry {
	return tool.CopySliceMap[any](items)
}

func (d *faultDestination) Summary() []string {
	return []string{"faultDestination"}
}

func (d *faultDestination) State() storage.State {
	return d.state
}

func (d *faultDestination) StateString() []string {
	return []string{d.state.Overview()}
}

// =====================================================================================
// faultProducerItem — implements producer.Item[MapEntry, MapEntry]
// =====================================================================================

var _ producerItemInterface = (*faultProducerItem)(nil)

// producerItemInterface is a type alias to avoid importing producer package
// in the type assertion; the struct is validated at compile time via the
// produceritem.NewRunner call in tests.
type producerItemInterface = interface {
	Prepare() error
	BeforeRun() error
	AfterRun() error
	Close() error
	AppendState()
	Concurrency() int
	Summary() string
	StateString() []string
	Output() []string
	Exec(storage.MapEntry) ([]storage.MapEntry, bool, error)
}

type faultProducerConfig struct {
	prepareErr   error
	beforeRunErr error
	execErr      error
	execAt       int64 // 0 = never inject; N = inject on Nth call
	afterRunErr  error
	closeErr     error
	concurrency  int
}

type faultProducerItem struct {
	testutilprocessor.TestProcessor

	cfg faultProducerConfig

	execCalled     atomic.Int64
	prepareCalled  atomic.Int64
	beforeRunCalled atomic.Int64
	afterRunCalled atomic.Int64
	closeCalled    atomic.Int64
}

func newFaultProducerItem(cfg faultProducerConfig) *faultProducerItem {
	if cfg.concurrency <= 0 {
		cfg.concurrency = 1
	}
	return &faultProducerItem{cfg: cfg}
}

func (f *faultProducerItem) Concurrency() int { return f.cfg.concurrency }

func (f *faultProducerItem) Summary() string { return "faultProducerItem" }

func (f *faultProducerItem) Prepare() error {
	f.prepareCalled.Add(1)
	return f.cfg.prepareErr
}

func (f *faultProducerItem) BeforeRun() error {
	f.beforeRunCalled.Add(1)
	return f.cfg.beforeRunErr
}

func (f *faultProducerItem) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	f.execCalled.Add(1)
	if f.cfg.execAt > 0 && f.execCalled.Load() == f.cfg.execAt {
		return nil, false, f.cfg.execErr
	}
	return []storage.MapEntry{item}, true, nil
}

func (f *faultProducerItem) AfterRun() error {
	f.afterRunCalled.Add(1)
	return f.cfg.afterRunErr
}

func (f *faultProducerItem) Close() error {
	f.closeCalled.Add(1)
	return f.cfg.closeErr
}

// =====================================================================================
// Sentinel errors for test assertions
// =====================================================================================

var (
	errSourcePrepare   = errors.New("source.Prepare failed")
	errSourceScan      = errors.New("source.Scan failed")
	errSourceFinish    = errors.New("source.Finish failed")
	errSourceClose     = errors.New("source.Close failed")
	errExec            = errors.New("executor.Exec failed")
	errAfterRun        = errors.New("processor.AfterRun failed")
	errBeforeRun       = errors.New("processor.BeforeRun failed")
	errProcPrepare     = errors.New("processor.Prepare failed")
	errProcClose       = errors.New("processor.Close failed")
	errDestPrepare     = errors.New("destination.Prepare failed")
	errDestReceive     = errors.New("destination.Receive failed")
	errDestFinish      = errors.New("destination.Finish failed")
	errDestClose       = errors.New("destination.Close failed")
)
