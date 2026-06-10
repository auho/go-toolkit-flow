package flow

import (
	"testing"

	work2 "github.com/auho/go-toolkit-flow/action/work"
)

func TestActionWork(t *testing.T) {
	opts := []Option[map[string]any]{
		WithSource[map[string]any](dataSource),
		WithActor[map[string]any](
			work2.NewActor[map[string]any](&work{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
