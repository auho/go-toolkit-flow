package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/operator/consumer"
)

var _ consumer.Batch[map[string]any] = (*batchOp)(nil)

type batchOp struct {
	operator.BaseOperator
}

func (b *batchOp) Concurrency() int {
	return runtime.NumCPU()
}

func (b *batchOp) AppendState() {}

func (b *batchOp) Summary() string {
	return "test batch"
}

func (b *batchOp) Prepare() error {
	b.SetStateLine(0, "prepare")
	return nil
}

func (b *batchOp) Exec(items []map[string]any) (int64, error) {
	for _, item := range items {
		_ = item
	}

	return int64(len(items)), nil
}

func (b *batchOp) BeforeExec() error {
	b.SetStateLine(0, "pre do")
	b.Outputln("pre do")

	return nil
}

func (b *batchOp) AfterExec() error {
	b.SetStateLine(0, "post do")
	b.Outputln("post do")

	return nil
}

func (b *batchOp) Close() error { return nil }
