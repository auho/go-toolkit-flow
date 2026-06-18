package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestRunners_New(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	if rs == nil {
		t.Error("NewRunners should not return nil")
	}
	if rs.Len() != 0 {
		t.Errorf("Len should be 0, got %d", rs.Len())
	}
}

func TestRunners_Add(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	r1 := newMockRunner[storage.MapEntry, storage.MapEntry](
		&mockExecutor[storage.MapEntry, storage.MapEntry]{},
		&mockOperator[storage.MapEntry]{},
	)

	rs.Add(r1)
	if rs.Len() != 1 {
		t.Errorf("Len should be 1, got %d", rs.Len())
	}

	r2 := newMockRunner[storage.MapEntry, storage.MapEntry](
		&mockExecutor[storage.MapEntry, storage.MapEntry]{},
		&mockOperator[storage.MapEntry]{},
	)
	r3 := newMockRunner[storage.MapEntry, storage.MapEntry](
		&mockExecutor[storage.MapEntry, storage.MapEntry]{},
		&mockOperator[storage.MapEntry]{},
	)
	rs.Add(r2, r3)
	if rs.Len() != 3 {
		t.Errorf("Len should be 3, got %d", rs.Len())
	}

	all := rs.All()
	if len(all) != 3 {
		t.Errorf("All should return 3 runners, got %d", len(all))
	}
}

func TestRunners_Prepare_AllSuccess(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	if op1.prepareCalled.Load() != 1 {
		t.Errorf("op1 prepareCalled should be 1, got %d", op1.prepareCalled.Load())
	}
	if op2.prepareCalled.Load() != 1 {
		t.Errorf("op2 prepareCalled should be 1, got %d", op2.prepareCalled.Load())
	}
}

func TestRunners_Prepare_OneFails(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{prepareErr: errors.New("prepare error")}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err == nil {
		t.Fatal("Prepare should return error")
	}
	if !contains(err.Error(), "prepare") {
		t.Errorf("error should contain 'prepare', got: %v", err)
	}
}

func TestRunners_Start(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	exec1 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k": "v"}}, amount: 1, affected: 1}
	exec2 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k2": "v2"}}, amount: 1, affected: 1}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](exec1, &mockOperator[storage.MapEntry]{}),
		newMockRunner[storage.MapEntry, storage.MapEntry](exec2, &mockOperator[storage.MapEntry]{}),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Receive([]storage.MapEntry{{"id": 1}})
	rs.Done()
	err = rs.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}

	if exec1.callCount.Load() != 1 {
		t.Errorf("exec1.callCount should be 1, got %d", exec1.callCount.Load())
	}
	if exec2.callCount.Load() != 1 {
		t.Errorf("exec2.callCount should be 1, got %d", exec2.callCount.Load())
	}
}

func TestRunners_Receive_Single(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	exec1 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k": "v"}}, amount: 1, affected: 1}
	op1 := &mockOperator[storage.MapEntry]{}

	rs.Add(newMockRunner[storage.MapEntry, storage.MapEntry](exec1, op1))

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Receive([]storage.MapEntry{{"id": 1}})
	rs.Done()
	err = rs.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}

	if exec1.callCount.Load() != 1 {
		t.Errorf("exec1.callCount should be 1, got %d", exec1.callCount.Load())
	}
}

func TestRunners_Receive_Multi(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	exec1 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k": "v"}}, amount: 1, affected: 1}
	exec2 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k2": "v2"}}, amount: 1, affected: 1}
	exec3 := &mockExecutor[storage.MapEntry, storage.MapEntry]{out: []storage.MapEntry{{"k3": "v3"}}, amount: 1, affected: 1}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](exec1, &mockOperator[storage.MapEntry]{}),
		newMockRunner[storage.MapEntry, storage.MapEntry](exec2, &mockOperator[storage.MapEntry]{}),
		newMockRunner[storage.MapEntry, storage.MapEntry](exec3, &mockOperator[storage.MapEntry]{}),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Receive([]storage.MapEntry{{"id": 1}})
	rs.Done()
	err = rs.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}

	if exec1.callCount.Load() != 1 {
		t.Errorf("exec1.callCount should be 1, got %d", exec1.callCount.Load())
	}
	if exec2.callCount.Load() != 1 {
		t.Errorf("exec2.callCount should be 1, got %d", exec2.callCount.Load())
	}
	if exec3.callCount.Load() != 1 {
		t.Errorf("exec3.callCount should be 1, got %d", exec3.callCount.Load())
	}
}

func TestRunners_Done(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Done()
	err = rs.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}
}

func TestRunners_Finish_AllSuccess(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{}),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{}),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Done()
	err = rs.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}
}

func TestRunners_Finish_OneFails(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{afterExecErr: errors.New("after exec error")}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	rs.Start()
	rs.Done()
	err = rs.Finish()
	if err == nil {
		t.Fatal("Finish should return error")
	}
	if !contains(err.Error(), "finish") {
		t.Errorf("error should contain 'finish', got: %v", err)
	}
}

func TestRunners_Close_AllSuccess(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	err := rs.Close()
	if err != nil {
		t.Fatalf("Close should succeed, got: %v", err)
	}

	if op1.closeCalled.Load() != 1 {
		t.Errorf("op1.closeCalled should be 1, got %d", op1.closeCalled.Load())
	}
	if op2.closeCalled.Load() != 1 {
		t.Errorf("op2.closeCalled should be 1, got %d", op2.closeCalled.Load())
	}
}

func TestRunners_Close_OneFails(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	op1 := &mockOperator[storage.MapEntry]{}
	op2 := &mockOperator[storage.MapEntry]{closeErr: errors.New("close error")}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op1),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, op2),
	)

	err := rs.Close()
	if err == nil {
		t.Fatal("Close should return error")
	}
	if !contains(err.Error(), "close") {
		t.Errorf("error should contain 'close', got: %v", err)
	}
}

func TestRunners_Summary(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{summaryStr: "summary1"}),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{summaryStr: "summary2"}),
	)

	summaries := rs.Summary()
	if len(summaries) != 2 {
		t.Errorf("Summary should have 2 entries, got %d", len(summaries))
	}
	if summaries[0] != "summary1" {
		t.Errorf("summaries[0] should be 'summary1', got '%s'", summaries[0])
	}
	if summaries[1] != "summary2" {
		t.Errorf("summaries[1] should be 'summary2', got '%s'", summaries[1])
	}
}

func TestRunners_State(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{summaryStr: "s1"}),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{summaryStr: "s2"}),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	state := rs.State()
	if len(state) == 0 {
		t.Error("State should not be empty")
	}
	// Should contain both summaries and state lines
	hasS1 := false
	hasS2 := false
	for _, s := range state {
		if s == "s1" {
			hasS1 = true
		}
		if s == "s2" {
			hasS2 = true
		}
	}
	if !hasS1 {
		t.Errorf("State should contain 's1', got: %v", state)
	}
	if !hasS2 {
		t.Errorf("State should contain 's2', got: %v", state)
	}
}

func TestRunners_Output(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{}),
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{}),
	)

	ctx := context.Background()
	err := rs.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	// Output returns nil when no output lines have been added; should not panic
	_ = rs.Output()
}

func TestRunners_Len(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	if rs.Len() != 0 {
		t.Errorf("Len should be 0, got %d", rs.Len())
	}

	rs.Add(
		newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{}),
	)
	if rs.Len() != 1 {
		t.Errorf("Len should be 1, got %d", rs.Len())
	}
}

func TestRunners_All(t *testing.T) {
	rs := NewRunners[storage.MapEntry, storage.MapEntry]()
	r1 := newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{})
	r2 := newMockRunner[storage.MapEntry, storage.MapEntry](&mockExecutor[storage.MapEntry, storage.MapEntry]{}, &mockOperator[storage.MapEntry]{})

	rs.Add(r1, r2)
	all := rs.All()
	if len(all) != 2 {
		t.Errorf("All should return 2 runners, got %d", len(all))
	}
}