package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
)

var _ operator.Batch[map[string]any] = (*batch)(nil)

type batch struct {
	operator.BaseOperator
}

func (b *batch) Concurrency() int {
	return runtime.NumCPU()
}

func (b *batch) AdditionalState() {}

func (b *batch) Summary() string {
	return "test batch"
}

func (b *batch) Prepare() error {
	b.SetStateLine(0, "prepare")
	return nil
}

func (b *batch) Exec(items []map[string]any) (int64, error) {
	for _, item := range items {
		_ = item
	}

	return int64(len(items)), nil
}

func (b *batch) BeforeExec() error {
	b.SetStateLine(0, "pre do")
	b.Outputln("pre do")

	return nil
}

func (b *batch) AfterExec() error {
	b.SetStateLine(0, "post do")
	b.Outputln("post do")

	return nil
}

func (b *batch) Close() error { return nil }
