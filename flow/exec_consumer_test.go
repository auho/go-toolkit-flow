package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/exec"
	consumerbatch "github.com/auho/go-toolkit-flow/v3/exec/consumer/batch"
	consumeritem "github.com/auho/go-toolkit-flow/v3/exec/consumer/item"
)

// TestExecConsumerItem verifies the consumer.Item path end-to-end:
// MySQL source → consumerItemOp (accept-all) → NoopDestination.
// Asserts the source data is fully read (source amount == DB row count).
func TestExecConsumerItem(t *testing.T) {
	table := "flow_consumer_item_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				consumeritem.NewRunner[map[string]any, map[string]any](&consumerItemOp{}),
			},
			// No destination → defaults to NoopDestination
		),
	}
	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	want := tableRowCount(t, table)
	if got := src.State().Amount(); got != want {
		t.Errorf("source amount = %d, want %d", got, want)
	}
}

// TestExecConsumerBatch verifies the consumer.Batch path end-to-end:
// MySQL source → batchOp (count) → NoopDestination.
// Asserts the source data is fully read (source amount == DB row count).
func TestExecConsumerBatch(t *testing.T) {
	table := "flow_consumer_batch_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				consumerbatch.NewRunner[map[string]any, map[string]any](&batchOp{}),
			},
			// No destination → defaults to NoopDestination
		),
	}
	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	want := tableRowCount(t, table)
	if got := src.State().Amount(); got != want {
		t.Errorf("source amount = %d, want %d", got, want)
	}
}

// tableRowCount returns the actual row count of the test table.
func tableRowCount(t *testing.T, table string) int64 {
	t.Helper()
	var count int64
	if err := _gormDB.Table(table).Count(&count).Error; err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return count
}
