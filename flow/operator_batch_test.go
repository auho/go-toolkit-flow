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

func (b *batch) RefreshState() {}

func (b *batch) Title() string {
	return "test batch"
}

func (b *batch) Prepare() error {
	b.SetState(0, "prepare")
	return nil
}

func (b *batch) Do(items []map[string]any) (int64, error) {
	for _, item := range items {
		_ = item
	}

	return int64(len(items)), nil
}

func (b *batch) BeforeRun() error {
	b.SetState(0, "pre do")
	b.Println("pre do")

	return nil
}

func (b *batch) AfterRun() error {
	b.SetState(0, "post do")
	b.Println("post do")

	return nil
}

func (b *batch) Close() error { return nil }
