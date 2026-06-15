package flow

import (
	"runtime"

	"github.com/auho/go-toolkit-flow/operator"
)

var _ operator.Transformer[map[string]any] = (*transformer)(nil)

type transformer struct {
	operator.BaseOperator
}

func (t *transformer) Concurrency() int {
	return runtime.NumCPU()
}

func (t *transformer) RefreshState() {}

func (t *transformer) Title() string {
	return "test transformer"
}

func (t *transformer) Prepare() error {
	t.SetState(0, "prepare")
	return nil
}

func (t *transformer) Do(item map[string]any) ([]map[string]any, bool, error) {
	return []map[string]any{item}, true, nil
}

func (t *transformer) PostBatchDo(items []map[string]any) error {
	for _, item := range items {
		_ = item
	}

	return nil
}

func (t *transformer) BeforeRun() error {
	t.SetState(0, "pre do")
	t.Println("pre do")

	return nil
}

func (t *transformer) AfterRun() error {
	t.SetState(0, "post do")
	t.Println("post do")

	return nil
}

func (t *transformer) Close() error { return nil }
