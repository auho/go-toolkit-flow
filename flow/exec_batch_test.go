package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec/consumer/batch"
)

func TestExecBatch(t *testing.T) {
	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](dataSource),
		WithRunner[map[string]any, map[string]any](
			batch.NewRunner[map[string]any, map[string]any](&batchOp{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
