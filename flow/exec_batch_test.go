package flow

import (
	"testing"

	batches "github.com/auho/go-toolkit-flow/exec/batch"
)

func TestExecBatch(t *testing.T) {
	opts := []Option[map[string]any]{
		WithSource[map[string]any](dataSource),
		WithRunner[map[string]any](
			batches.NewRunner[map[string]any](&batch{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
