package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/producer/item"
)

func TestExecProducerItem(t *testing.T) {
	table := "flow_producer_item_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				item.NewRunner[map[string]any, map[string]any](&transformer{}),
			},
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}