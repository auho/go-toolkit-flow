package flow

import (
	"testing"

	transformers "github.com/auho/go-toolkit-flow/exec/transformer"
)

func TestActionSingleton(t *testing.T) {
	opts := []Option[map[string]any]{
		WithSource[map[string]any](dataSource),
		WithRunner[map[string]any](
			transformers.NewRunner[map[string]any](&transformer{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}