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

	if state := d.State(); state == nil {
		t.Errorf("State() returned nil, want non-nil")
	}
}

// TestNoopDestination_State_NonNil verifies that State() returns a non-nil
// State. Before #3 fix: State() returned nil, causing MultiSnapshot.Overview()
// to nil-dereference panic when collected by MultiDestination.State().
func TestNoopDestination_State_NonNil(t *testing.T) {
	var d NoopDestination[string]

	state := d.State()
	if state == nil {
		t.Fatal("State() returned nil, want non-nil State")
	}

	// Should not panic
	_ = state.Overview()
	_ = state.Amount()
	_ = state.Title()
	_ = state.Concurrency()
}

// TestMultiDestination_WithNoopDestination_State verifies that a
// MultiDestination containing a NoopDestination does not panic when
// State().Overview() is called. Before #3 fix: NoopDestination.State()
// returned nil, causing MultiSnapshot.Overview() to panic.
func TestMultiDestination_WithNoopDestination_State(t *testing.T) {
	md := MultiDestination[string]{
		NoopDestination[string]{},
	}

	state := md.State()
	if state == nil {
		t.Fatal("State() returned nil")
	}

	// Should not panic — this is the core regression test for #3
	_ = state.Overview()
	_ = state.Amount()
	_ = state.Title()
	_ = state.Concurrency()
}
