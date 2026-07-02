package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/exec"
	producerbatch "github.com/auho/go-toolkit-flow/v3/exec/producer/batch"
	produceritem "github.com/auho/go-toolkit-flow/v3/exec/producer/item"
	mockdest "github.com/auho/go-toolkit-flow/v3/storage/mock/destination"
)

// TestExecProducerItem verifies the producer.Item path end-to-end:
// MySQL source → itemOp (1:1 passthrough) → InsertMap destination.
// Asserts source amount and destination amount both equal the DB row count.
func TestExecProducerItem(t *testing.T) {
	table := "flow_producer_item_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	dest, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest,
		),
	}
	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	want := tableRowCount(t, table)
	if got := src.State().Amount(); got != want {
		t.Errorf("source amount = %d, want %d", got, want)
	}
	if got := dest.State().Amount(); got != want {
		t.Errorf("destination amount = %d, want %d", got, want)
	}
	if got := int64(len(dest.Items())); got != want {
		t.Errorf("items length = %d, want %d", got, want)
	}
}

// TestExecProducerBatch verifies the producer.Batch path end-to-end:
// MySQL source → producerBatchOp (batch passthrough) → InsertMap destination.
// Asserts source amount and destination amount both equal the DB row count.
func TestExecProducerBatch(t *testing.T) {
	table := "flow_producer_batch_source"
	setupMySQLTable(table)
	t.Cleanup(func() { teardownMySQLTable(table) })

	src := buildDataSource(table)

	dest, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				producerbatch.NewRunner[map[string]any, map[string]any](&producerBatchOp{}),
			},
			dest,
		),
	}
	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	want := tableRowCount(t, table)
	if got := src.State().Amount(); got != want {
		t.Errorf("source amount = %d, want %d", got, want)
	}
	if got := dest.State().Amount(); got != want {
		t.Errorf("destination amount = %d, want %d", got, want)
	}
	if got := int64(len(dest.Items())); got != want {
		t.Errorf("items length = %d, want %d", got, want)
	}
}
