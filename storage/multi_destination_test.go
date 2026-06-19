package storage

import (
	"context"
	"strings"
	"testing"
)

// spyDestination records method calls for testing MultiDestination.
type spyDestination struct {
	name       string
	prepareErr error
	receiveErr error
	finishErr  error
	closeErr   error
	prepared   bool
	accepted   int
	received   [][]string
	doneCount  int
	finished   bool
	closed     bool
}

func (s *spyDestination) Prepare(ctx context.Context) error {
	s.prepared = true
	return s.prepareErr
}

func (s *spyDestination) Accept() {
	s.accepted++
}

func (s *spyDestination) Receive(items []string) error {
	s.received = append(s.received, items)
	return s.receiveErr
}

func (s *spyDestination) Done() {
	s.doneCount++
}

func (s *spyDestination) Finish() error {
	s.finished = true
	return s.finishErr
}

func (s *spyDestination) Close() error {
	s.closed = true
	return s.closeErr
}

func (s *spyDestination) Summary() []string {
	return []string{s.name}
}

func (s *spyDestination) StateInfo() StateInfo {
	st := NewState()
	st.SetStatus(s.name)
	return st
}

func TestMultiDestination_Prepare_AllSuccess(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Prepare(context.Background())
	if err != nil {
		t.Errorf("Prepare() returned error: %v", err)
	}
	if !d1.prepared {
		t.Error("d1 was not prepared")
	}
	if !d2.prepared {
		t.Error("d2 was not prepared")
	}
}

func TestMultiDestination_Prepare_OneFails(t *testing.T) {
	d1 := &spyDestination{name: "d1", prepareErr: context.Canceled}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Prepare(context.Background())
	if err == nil {
		t.Fatal("Prepare() expected error, got nil")
	}
	if !d1.prepared {
		t.Error("d1 was not prepared")
	}
	if d2.prepared {
		t.Error("d2 was prepared despite d1 error (short-circuit expected)")
	}
}

func TestMultiDestination_Accept(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	md.Accept()
	if d1.accepted != 1 {
		t.Errorf("d1.accepted = %d, want 1", d1.accepted)
	}
	if d2.accepted != 1 {
		t.Errorf("d2.accepted = %d, want 1", d2.accepted)
	}
}

func TestMultiDestination_Receive_AllSuccess(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	d3 := &spyDestination{name: "d3"}
	md := MultiDestination[string]{d1, d2, d3}

	items := []string{"a", "b"}
	err := md.Receive(items)
	if err != nil {
		t.Errorf("Receive() returned error: %v", err)
	}

	for i, d := range []*spyDestination{d1, d2, d3} {
		if len(d.received) != 1 {
			t.Errorf("d%d.received count = %d, want 1", i+1, len(d.received))
			continue
		}
		got := d.received[0]
		if len(got) != 2 || got[0] != "a" || got[1] != "b" {
			t.Errorf("d%d.received[0] = %v, want [a b]", i+1, got)
		}
	}
}

func TestMultiDestination_Receive_OneFails(t *testing.T) {
	d1 := &spyDestination{name: "d1", receiveErr: context.Canceled}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	items := []string{"a", "b"}
	err := md.Receive(items)
	if err == nil {
		t.Fatal("Receive() expected error, got nil")
	}

	if len(d1.received) != 1 {
		t.Errorf("d1.received count = %d, want 1", len(d1.received))
	}
	if len(d2.received) != 0 {
		t.Error("d2.received should be empty (short-circuit expected)")
	}
}

func TestMultiDestination_Done(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	md.Done()
	if d1.doneCount != 1 {
		t.Errorf("d1.doneCount = %d, want 1", d1.doneCount)
	}
	if d2.doneCount != 1 {
		t.Errorf("d2.doneCount = %d, want 1", d2.doneCount)
	}
}

func TestMultiDestination_Finish_AllSuccess(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Finish()
	if err != nil {
		t.Errorf("Finish() returned error: %v", err)
	}
	if !d1.finished {
		t.Error("d1 was not finished")
	}
	if !d2.finished {
		t.Error("d2 was not finished")
	}
}

func TestMultiDestination_Finish_OneFails(t *testing.T) {
	d1 := &spyDestination{name: "d1", finishErr: context.Canceled}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Finish()
	if err == nil {
		t.Fatal("Finish() expected error, got nil")
	}
	if !d1.finished {
		t.Error("d1 was not finished")
	}
	if d2.finished {
		t.Error("d2 was finished despite d1 error (short-circuit expected)")
	}
}

func TestMultiDestination_Close_AllSuccess(t *testing.T) {
	d1 := &spyDestination{name: "d1"}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
	if !d1.closed {
		t.Error("d1 was not closed")
	}
	if !d2.closed {
		t.Error("d2 was not closed")
	}
}

func TestMultiDestination_Close_OneFails(t *testing.T) {
	d1 := &spyDestination{name: "d1", closeErr: context.Canceled}
	d2 := &spyDestination{name: "d2"}
	md := MultiDestination[string]{d1, d2}

	err := md.Close()
	if err == nil {
		t.Fatal("Close() expected error, got nil")
	}
	if !d1.closed {
		t.Error("d1 was not closed")
	}
	if d2.closed {
		t.Error("d2 was closed despite d1 error (short-circuit expected)")
	}
}

func TestMultiDestination_Summary(t *testing.T) {
	d1 := &spyDestination{name: "alpha"}
	d2 := &spyDestination{name: "beta"}
	md := MultiDestination[string]{d1, d2}

	summary := md.Summary()
	if len(summary) != 2 {
		t.Fatalf("Summary() length = %d, want 2", len(summary))
	}
	if summary[0] != "alpha" {
		t.Errorf("summary[0] = %q, want %q", summary[0], "alpha")
	}
	if summary[1] != "beta" {
		t.Errorf("summary[1] = %q, want %q", summary[1], "beta")
	}
}

func TestMultiDestination_State(t *testing.T) {
	d1 := &spyDestination{name: "s1"}
	d2 := &spyDestination{name: "s2"}
	d3 := &spyDestination{name: "s3"}
	md := MultiDestination[string]{d1, d2, d3}

	si := md.StateInfo()
	if si == nil {
		t.Fatal("StateInfo() returned nil")
	}
	overview := si.Overview()
	for _, name := range []string{"s1", "s2", "s3"} {
		if !strings.Contains(overview, name) {
			t.Errorf("StateInfo().Overview() = %q, missing %q", overview, name)
		}
	}
}

func TestMultiDestination_Empty(t *testing.T) {
	md := MultiDestination[string]{}

	_ = md.Prepare(context.Background())
	md.Accept()
	_ = md.Receive([]string{"a"})
	md.Done()
	_ = md.Finish()
	_ = md.Close()
	_ = md.Summary()
	_ = md.StateInfo()
}