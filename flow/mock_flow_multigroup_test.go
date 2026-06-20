package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/producer/item"
	mockdest "github.com/auho/go-toolkit-flow/storage/mock/destination"
	mocksrc "github.com/auho/go-toolkit-flow/storage/mock/source"
)

// TestMockFlow_MultiGroup verifies the multi-group pipeline:
// mock source → fan-out → [group1: itemOp → dest1, group2: itemOp → dest2].
//
// Each group receives the full dataset (source fans out to all groups),
// so both destinations must end up with the same complete data.
// This exercises the converged group.Prepare/Start(accept)/Receive/
// OutputForward/Finish/Close path under multiple groups.
func TestMockFlow_MultiGroup(t *testing.T) {
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
				item.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest1,
		),
		WithGroup[map[string]any, map[string]any](
			[]exec.Runner[map[string]any, map[string]any]{
				item.NewRunner[map[string]any, map[string]any](&itemOp{}),
			},
			dest2,
		),
	}

	if err := RunFlow(opts...); err != nil {
		t.Fatal(err)
	}

	// Source must have generated the full dataset
	if src.State().Amount() != total {
		t.Errorf("source amount = %d, want %d", src.State().Amount(), total)
	}

	// Each group's destination must have received the full dataset
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
