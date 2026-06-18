package flow

import (
	"testing"

	"github.com/auho/go-toolkit-flow/exec/producer/item"
)

func TestExecTransformer(t *testing.T) {
	opts := []Option[map[string]any, map[string]any]{
		WithSource[map[string]any, map[string]any](dataSource),
		WithRunner[map[string]any, map[string]any](
			item.NewRunner[map[string]any, map[string]any](&transformer{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
