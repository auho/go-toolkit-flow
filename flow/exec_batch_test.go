package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/consumer/batch"
)

func TestExecBatch(t *testing.T) {
	buildDataSource()

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](dataSource),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				batch.NewRunner[map[string]any, map[string]any](&batchOp{}),
			},
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
