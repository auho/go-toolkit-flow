package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec"
	"github.com/auho/go-toolkit-flow/exec/producer/item"
)

func TestExecTransformer(t *testing.T) {
	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](dataSource),
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
