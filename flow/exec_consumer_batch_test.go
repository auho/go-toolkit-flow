package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/exec"
	"github.com/auho/go-toolkit-flow/v3/exec/consumer/batch"
)

func TestExecConsumerBatch(t *testing.T) {
	table := "flow_consumer_batch_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
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
