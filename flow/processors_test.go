package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/processor"
	"github.com/auho/go-toolkit-flow/processor/consumer"
	"github.com/auho/go-toolkit-flow/processor/producer"
	"github.com/auho/go-toolkit-flow/storage"
)

// This file defines helper processors and a SliceEntry source format used by
// the flow combination tests (mock_flow_test.go). All helpers target
// storage.MapEntry unless noted, to match mock source/destination defaults.

// === Producer-path processors ===

// itemOp is a producer.Item that passes each input item through unchanged (1:1).
var _ producer.Item[map[string]any, map[string]any] = (*itemOp)(nil)

type itemOp struct {
	processor.BaseProcessor
}

func (t *itemOp) Concurrency() int { return runtime.NumCPU() }
func (t *itemOp) AppendState()     {}

func (t *itemOp) Summary() string { return "test itemOp" }

func (t *itemOp) Prepare() error {
	t.Outputln("Prepare")
	return nil
}

func (t *itemOp) Exec(item map[string]any) ([]map[string]any, bool, error) {
	return []map[string]any{item}, true, nil
}

func (t *itemOp) BeforeRun() error {
	t.Outputln("BeforeRun")
	return nil
}

func (t *itemOp) AfterRun() error {
	t.Outputln("AfterRun")
	return nil
}

func (t *itemOp) Close() error { return nil }

// producerBatchOp is a producer.Batch that passes the input batch through unchanged.
var _ producer.Batch[map[string]any, map[string]any] = (*producerBatchOp)(nil)

type producerBatchOp struct {
	processor.BaseProcessor
}

func (p *producerBatchOp) Concurrency() int { return runtime.NumCPU() }
func (p *producerBatchOp) AppendState()     {}

func (p *producerBatchOp) Summary() string { return "test producerBatchOp" }

func (p *producerBatchOp) Prepare() error {
	p.Outputln("Prepare")
	return nil
}

func (p *producerBatchOp) Exec(items []map[string]any) ([]map[string]any, int64, error) {
	return items, int64(len(items)), nil
}

func (p *producerBatchOp) BeforeRun() error {
	p.Outputln("BeforeRun")
	return nil
}

func (p *producerBatchOp) AfterRun() error {
	p.Outputln("AfterRun")
	return nil
}

func (p *producerBatchOp) Close() error { return nil }

// === Consumer-path processors ===

// consumerItemOp is a consumer.Item that accepts every item (ok=true).
var _ consumer.Item[map[string]any] = (*consumerItemOp)(nil)

type consumerItemOp struct {
	processor.BaseProcessor
}

func (c *consumerItemOp) Concurrency() int { return runtime.NumCPU() }
func (c *consumerItemOp) AppendState()     {}

func (c *consumerItemOp) Summary() string { return "test consumerItemOp" }

func (c *consumerItemOp) Prepare() error {
	c.Outputln("Prepare")
	return nil
}

func (c *consumerItemOp) Exec(item map[string]any) (bool, error) {
	return true, nil
}

func (c *consumerItemOp) BeforeRun() error {
	c.Outputln("BeforeRun")
	return nil
}

func (c *consumerItemOp) AfterRun() error {
	c.Outputln("AfterRun")
	return nil
}

func (c *consumerItemOp) Close() error { return nil }

// batchOp is a consumer.Batch that counts processed items.
var _ consumer.Batch[map[string]any] = (*batchOp)(nil)

type batchOp struct {
	processor.BaseProcessor
}

func (b *batchOp) Concurrency() int { return runtime.NumCPU() }
func (b *batchOp) AppendState()     {}

func (b *batchOp) Summary() string { return "test batchOp" }

func (b *batchOp) Prepare() error {
	b.Outputln("prepare")
	return nil
}

func (b *batchOp) Exec(items []map[string]any) (int64, error) {
	return int64(len(items)), nil
}

func (b *batchOp) BeforeRun() error {
	b.Outputln("BeforeRun")
	return nil
}

func (b *batchOp) AfterRun() error {
	b.Outputln("AfterRun")
	return nil
}

func (b *batchOp) Close() error { return nil }

// sliceItemOp is a producer.Item that passes each SliceEntry through unchanged (1:1).
var _ producer.Item[storage.SliceEntry, storage.SliceEntry] = (*sliceItemOp)(nil)

type sliceItemOp struct {
	processor.BaseProcessor
}

func (t *sliceItemOp) Concurrency() int { return runtime.NumCPU() }
func (t *sliceItemOp) AppendState()     {}

func (t *sliceItemOp) Summary() string { return "test sliceItemOp" }

func (t *sliceItemOp) Prepare() error {
	t.Outputln("Prepare")
	return nil
}

func (t *sliceItemOp) Exec(item storage.SliceEntry) ([]storage.SliceEntry, bool, error) {
	return []storage.SliceEntry{item}, true, nil
}

func (t *sliceItemOp) BeforeRun() error {
	t.Outputln("BeforeRun")
	return nil
}

func (t *sliceItemOp) AfterRun() error {
	t.Outputln("AfterRun")
	return nil
}

func (t *sliceItemOp) Close() error { return nil }
