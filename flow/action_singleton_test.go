package flow

import (
	"testing"

	singleton2 "github.com/auho/go-toolkit-flow/action/singleton"
)

func TestActionSingleton(t *testing.T) {
	opts := []Option[map[string]any]{
		WithSource[map[string]any](dataSource),
		WithActor[map[string]any](
			singleton2.NewActor[map[string]any](&singleton{}),
		),
	}
	err := RunFlow(opts...)
	if err != nil {
		t.Fatal(err)
	}
}
