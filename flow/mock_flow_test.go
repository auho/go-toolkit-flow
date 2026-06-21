package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/consumer/batch"
	consumeritem "github.com/auho/go-toolkit-flow/exec/consumer/item"
	producerbatch "github.com/auho/go-toolkit-flow/exec/producer/batch"
	produceritem "github.com/auho/go-toolkit-flow/exec/producer/item"
	"github.com/auho/go-toolkit-flow/storage"
	mockdest "github.com/auho/go-toolkit-flow/storage/mock/destination"
	mocksrc "github.com/auho/go-toolkit-flow/storage/mock/source"
)

// This file contains combination tests for the flow package. All tests use
// storage.mock source/destination only — no third-party dependencies.
//
// Coverage matrix:
//   - processor kind: producer.Item / producer.Batch / consumer.Item / consumer.Batch
//   - destination:    NoopDestination / InsertMap / UpdateMap / MultiDestination
//   - group layout:   single-group single-runner / single-group multi-runner / multi-group
//   - entry type:     MapEntry / SliceEntry

// === Processor × destination combinations ===

// TestFlow_ProducerItem verifies the producer.Item path:
// mock source → itemOp (1:1 passthrough) → InsertMap destination.
// Asserts count consistency and field content integrity.
func TestFlow_ProducerItem(t *testing.T) {
	total := int64(100)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 10})

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

	// Count consistency: source generated == configured total == destination received
	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
	if dest.StateInfo().Amount() != total {
		t.Errorf("destination amount = %d, want %d", dest.StateInfo().Amount(), total)
	}

	// Content integrity: each id in [1, total], unique; content non-zero
	items := dest.Items()
	if int64(len(items)) != total {
		t.Fatalf("items length = %d, want %d", len(items), total)
	}

	seenIDs := make(map[int64]bool, total)
	for i, item := range items {
		id, ok := item["id"].(int64)
		if !ok {
			t.Errorf("item %d: id type = %T, want int64", i, item["id"])
			continue
		}
		if id < 1 || id > total {
			t.Errorf("item %d: id = %d, want in range [1, %d]", i, id, total)
		}
		if seenIDs[id] {
			t.Errorf("item %d: duplicate id = %d", i, id)
		}
		seenIDs[id] = true

		content, ok := item["content"].(int64)
		if !ok {
			t.Errorf("item %d: content type = %T, want int64", i, item["content"])
			continue
		}
		if content == 0 {
			t.Errorf("item %d: content is zero", i)
		}
	}

	if int64(len(seenIDs)) != total {
		t.Errorf("unique IDs = %d, want %d", len(seenIDs), total)
	}
}

// TestFlow_ProducerBatch verifies the producer.Batch path:
// mock source → producerBatchOp (batch passthrough) → InsertMap destination.
// Asserts count consistency (source total == destination received).
func TestFlow_ProducerBatch(t *testing.T) {
	total := int64(200)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 25})

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

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
	if dest.StateInfo().Amount() != total {
		t.Errorf("destination amount = %d, want %d", dest.StateInfo().Amount(), total)
	}
}

// TestFlow_ConsumerItem verifies the consumer.Item path:
// mock source → consumerItemOp (accept-all) → NoopDestination (no dest registered).
// Asserts the source data is fully generated and the flow completes.
func TestFlow_ConsumerItem(t *testing.T) {
	total := int64(150)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 15})

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

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
}

// TestFlow_ConsumerBatch verifies the consumer.Batch path:
// mock source → batchOp (count) → NoopDestination (no dest registered).
// Asserts the source data is fully generated and the flow completes.
func TestFlow_ConsumerBatch(t *testing.T) {
	total := int64(300)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 30})

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				batch.NewRunner[map[string]any, map[string]any](&batchOp{}),
			},
			// No destination → defaults to NoopDestination
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
}

// === Destination & group layout combinations ===

// TestFlow_MultiDestination verifies fan-out within a single group:
// mock source → itemOp → 2 × InsertMap (wrapped as MultiDestination).
// Both destinations must receive the same complete dataset.
func TestFlow_MultiDestination(t *testing.T) {
	total := int64(200)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 25})

	dest1, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}
	dest2, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest1, dest2, // WithGroup wraps multiple dests as MultiDestination
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	if dest1.StateInfo().Amount() != total {
		t.Errorf("dest1 amount = %d, want %d", dest1.StateInfo().Amount(), total)
	}
	if dest2.StateInfo().Amount() != total {
		t.Errorf("dest2 amount = %d, want %d", dest2.StateInfo().Amount(), total)
	}

	// Both destinations must have identical items (same IDs in the same order)
	items1 := dest1.Items()
	items2 := dest2.Items()
	if len(items1) != len(items2) {
		t.Fatalf("items length mismatch: dest1=%d, dest2=%d", len(items1), len(items2))
	}

	mismatchCount := 0
	for i := range items1 {
		id1, ok1 := items1[i]["id"].(int64)
		id2, ok2 := items2[i]["id"].(int64)
		if !ok1 || !ok2 || id1 != id2 {
			mismatchCount++
		}
	}
	if mismatchCount > 0 {
		t.Errorf("items mismatch between dest1 and dest2: %d items differ", mismatchCount)
	}
}

// TestFlow_MultiGroup verifies fan-out across multiple groups:
// mock source → [group1: itemOp → dest1, group2: itemOp → dest2].
// Each group receives the full dataset (source fans out to all groups).
func TestFlow_MultiGroup(t *testing.T) {
	total := int64(300)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 30})

	dest1, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}
	dest2, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest1,
		),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest2,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
	if dest1.StateInfo().Amount() != total {
		t.Errorf("dest1 amount = %d, want %d", dest1.StateInfo().Amount(), total)
	}
	if dest2.StateInfo().Amount() != total {
		t.Errorf("dest2 amount = %d, want %d", dest2.StateInfo().Amount(), total)
	}

	// Both destinations must have identical items (same IDs in the same order)
	items1 := dest1.Items()
	items2 := dest2.Items()
	if len(items1) != len(items2) {
		t.Fatalf("items length mismatch: dest1=%d, dest2=%d", len(items1), len(items2))
	}

	mismatchCount := 0
	for i := range items1 {
		id1, ok1 := items1[i]["id"].(int64)
		id2, ok2 := items2[i]["id"].(int64)
		if !ok1 || !ok2 || id1 != id2 {
			mismatchCount++
		}
	}
	if mismatchCount > 0 {
		t.Errorf("items mismatch between dest1 and dest2: %d items differ", mismatchCount)
	}
}

// TestFlow_MultiRunner verifies fan-out within a single group to multiple runners:
// mock source → 2 × itemOp (fan-out with copy) → fan-in → InsertMap.
// With TotalRunners > 1, each runner receives a copy of every batch, so the
// destination collects 2×total items (group-internal fan-out semantics).
func TestFlow_MultiRunner(t *testing.T) {
	total := int64(100)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 10})

	dest, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
				produceritem.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	// Each runner receives a copy of every batch → destination gets 2×total.
	want := total * 2
	if dest.StateInfo().Amount() != want {
		t.Errorf("destination amount = %d, want %d (2×%d)", dest.StateInfo().Amount(), want, total)
	}
}

// TestFlow_DestinationUpdate verifies the UpdateMap destination path:
// mock source → itemOp → UpdateMap destination.
// Asserts count consistency (source total == destination received).
func TestFlow_DestinationUpdate(t *testing.T) {
	total := int64(120)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 20})

	dest, err := mockdest.NewUpdateMap()
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

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
	if dest.StateInfo().Amount() != total {
		t.Errorf("destination amount = %d, want %d", dest.StateInfo().Amount(), total)
	}
}

// === Entry type combinations ===

// TestFlow_SliceEntry verifies the SliceEntry path end-to-end:
// mock source (SliceEntry) → sliceItemOp (SliceEntry passthrough) → InsertSlice destination.
// Asserts count consistency (source total == destination received).
func TestFlow_SliceEntry(t *testing.T) {
	total := int64(80)
	src := mocksrc.NewSlice(mocksrc.Config{Total: total, PageSize: 10})

	dest, err := mockdest.NewInsertSlice()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[storage.SliceEntry, storage.SliceEntry]{
		WithSource[storage.SliceEntry, storage.SliceEntry](src),
		WithGroup[storage.SliceEntry, storage.SliceEntry](
			[]exec.Runner[storage.SliceEntry, storage.SliceEntry]{
				produceritem.NewRunner[storage.SliceEntry, storage.SliceEntry](&sliceItemOp{}),
			},
			dest,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}
	if dest.StateInfo().Amount() != total {
		t.Errorf("destination amount = %d, want %d", dest.StateInfo().Amount(), total)
	}
}
