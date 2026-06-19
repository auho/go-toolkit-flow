package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/consumer/batch"
	"github.com/auho/go-toolkit-flow/exec/producer/item"
	mockdest "github.com/auho/go-toolkit-flow/storage/mock/destination"
	mocksrc "github.com/auho/go-toolkit-flow/storage/mock/source"
)

// TestMockFlow_Batch verifies the consumer path: mock source → batch consumer → NoopDestination.
// No destination is registered, so WithGroup defaults to NoopDestination.
// Verifies that all source data is generated and the flow completes without error.
func TestMockFlow_Batch(t *testing.T) {
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

	// Verify all source data was generated
	if src.StateInfo().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.StateInfo().Amount(), total)
	}
}

// TestMockFlow_Transformer_Count verifies count consistency through the full pipeline:
// mock source → transformer (1:1 passthrough) → mock destination.
// Asserts: source total == source generated == destination received.
func TestMockFlow_Transformer_Count(t *testing.T) {
	total := int64(500)
	src := mocksrc.NewMap(mocksrc.Config{Total: total, PageSize: 50})

	dest, err := mockdest.NewInsertMap()
	if err != nil {
		t.Fatal(err)
	}

	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](src),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				item.NewRunner[map[string]any, map[string]any](&transformer{}),
			},
			dest,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	// Count consistency: source generated == configured total
	if src.StateInfo().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.StateInfo().Amount(), total)
	}
	// Count consistency: destination received == source generated
	if dest.StateInfo().Amount() != total {
		t.Errorf("destination amount = %d, want %d", dest.StateInfo().Amount(), total)
	}
}

// TestMockFlow_Transformer_Content verifies field content integrity:
// each received item must have an "id" field with a unique value in [1, total],
// and a non-zero "content" field.
func TestMockFlow_Transformer_Content(t *testing.T) {
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
				item.NewRunner[map[string]any, map[string]any](&transformer{}),
			},
			dest,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	items := dest.Items()
	if int64(len(items)) != total {
		t.Fatalf("items length = %d, want %d", len(items), total)
	}

	// Verify each item: id is in [1, total] and unique; content is non-zero
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

	// All IDs must be present (no data loss)
	if int64(len(seenIDs)) != total {
		t.Errorf("unique IDs = %d, want %d", len(seenIDs), total)
	}
}

// TestMockFlow_MultiDestination verifies fan-out: mock source → transformer → 2 destinations.
// Both destinations must receive the same complete data (count + content).
func TestMockFlow_MultiDestination(t *testing.T) {
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
				item.NewRunner[map[string]any, map[string]any](&transformer{}),
			},
			dest1, dest2, // WithGroup wraps multiple dests as MultiDestination
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	// Both destinations must have received the full dataset
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
