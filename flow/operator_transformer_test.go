package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/operator/producer"
)

var _ producer.Item[map[string]any, map[string]any] = (*transformer)(nil)

type transformer struct {
	operator.BaseOperator
}

func (t *transformer) Concurrency() int {
	return runtime.NumCPU()
}

func (t *transformer) AppendState() {}

func (t *transformer) Summary() string {
	return "test transformer"
}

func (t *transformer) Prepare() error {
	t.SetStateLine(0, "prepare")
	return nil
}

func (t *transformer) Exec(item map[string]any) ([]map[string]any, bool, error) {
	return []map[string]any{item}, true, nil
}

func (t *transformer) PostBatchExec(items []map[string]any) error {
	for _, item := range items {
		_ = item
	}

	return nil
}

func (t *transformer) BeforeExec() error {
	t.SetStateLine(0, "pre do")
	t.Outputln("pre do")

	return nil
}

func (t *transformer) AfterExec() error {
	t.SetStateLine(0, "post do")
	t.Outputln("post do")

	return nil
}

func (t *transformer) Close() error { return nil }
