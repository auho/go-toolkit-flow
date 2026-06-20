package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/operator/consumer"
	"github.com/auho/go-toolkit-flow/operator/producer"
	"github.com/auho/go-toolkit-flow/storage"
)

// This file defines helper operators and a SliceEntry source format used by
// the flow combination tests (mock_flow_test.go). All helpers target
// storage.MapEntry unless noted, to match mock source/destination defaults.

// === Producer-path operators ===

// itemOp is a producer.Item that passes each input item through unchanged (1:1).
var _ producer.Item[map[string]any, map[string]any] = (*itemOp)(nil)

type itemOp struct {
	operator.BaseOperator
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

func (t *itemOp) PostBatchExec(items []map[string]any) error {
	return nil
}

func (t *itemOp) BeforeExec() error {
	t.Outputln("BeforeExec")
	return nil
}

func (t *itemOp) AfterExec() error {
	t.Outputln("AfterExec")
	return nil
}

func (t *itemOp) Close() error { return nil }

// producerBatchOp is a producer.Batch that passes the input batch through unchanged.
var _ producer.Batch[map[string]any, map[string]any] = (*producerBatchOp)(nil)

type producerBatchOp struct {
	operator.BaseOperator
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

func (p *producerBatchOp) BeforeExec() error {
	p.Outputln("BeforeExec")
	return nil
}

func (p *producerBatchOp) AfterExec() error {
	p.Outputln("AfterExec")
	return nil
}

func (p *producerBatchOp) Close() error { return nil }

// === Consumer-path operators ===

// consumerItemOp is a consumer.Item that accepts every item (ok=true).
var _ consumer.Item[map[string]any] = (*consumerItemOp)(nil)

type consumerItemOp struct {
	operator.BaseOperator
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

func (c *consumerItemOp) BeforeExec() error {
	c.Outputln("BeforeExec")
	return nil
}

func (c *consumerItemOp) AfterExec() error {
	c.Outputln("AfterExec")
	return nil
}

func (c *consumerItemOp) Close() error { return nil }

// batchOp is a consumer.Batch that counts processed items.
var _ consumer.Batch[map[string]any] = (*batchOp)(nil)

type batchOp struct {
	operator.BaseOperator
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

func (b *batchOp) BeforeExec() error {
	b.Outputln("BeforeExec")
	return nil
}

func (b *batchOp) AfterExec() error {
	b.Outputln("AfterExec")
	return nil
}

func (b *batchOp) Close() error { return nil }

// sliceItemOp is a producer.Item that passes each SliceEntry through unchanged (1:1).
var _ producer.Item[storage.SliceEntry, storage.SliceEntry] = (*sliceItemOp)(nil)

type sliceItemOp struct {
	operator.BaseOperator
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

func (t *sliceItemOp) PostBatchExec(items []storage.SliceEntry) error {
	return nil
}

func (t *sliceItemOp) BeforeExec() error {
	t.Outputln("BeforeExec")
	return nil
}

func (t *sliceItemOp) AfterExec() error {
	t.Outputln("AfterExec")
	return nil
}

func (t *sliceItemOp) Close() error { return nil }
