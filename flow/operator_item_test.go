package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
	"github.com/auho/go-toolkit-flow/operator/producer"
)

var _ producer.Item[map[string]any, map[string]any] = (*itemOp)(nil)

type itemOp struct {
	operator.BaseOperator
}

func (t *itemOp) Concurrency() int {
	return runtime.NumCPU()
}

func (t *itemOp) AppendState() {}

func (t *itemOp) Summary() string {
	return "test itemOp"
}

func (t *itemOp) Prepare() error {
	t.Outputln("Prepare")

	return nil
}

func (t *itemOp) Exec(item map[string]any) ([]map[string]any, bool, error) {
	return []map[string]any{item}, true, nil
}

func (t *itemOp) PostBatchExec(items []map[string]any) error {
	for _, item := range items {
		_ = item
	}

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
