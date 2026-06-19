package storage

import (
	"context"
	"testing"
)

func TestNoopDestination_All(t *testing.T) {
	var d NoopDestination[string]

	if err := d.Prepare(context.Background()); err != nil {
		t.Errorf("Prepare() returned error: %v", err)
	}

	d.Accept()

	if err := d.Receive([]string{"a", "b"}); err != nil {
		t.Errorf("Receive() returned error: %v", err)
	}

	d.Done()

	if err := d.Finish(); err != nil {
		t.Errorf("Finish() returned error: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}

	if summary := d.Summary(); summary != nil {
		t.Errorf("Summary() = %v, want nil", summary)
	}

	if state := d.StateInfo(); state != nil {
		t.Errorf("State() = %v, want nil", state)
	}
}