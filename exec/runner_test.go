package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestRunner_New(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	if r == nil {
		t.Error("NewRunner should not return nil")
	}

	ch := r.OutChan()
	if ch == nil {
		t.Error("OutChan should not return nil")
	}
}

func TestRunner_Prepare_Success(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	if operator.prepareCalled.Load() != 1 {
		t.Errorf("prepareCalled should be 1, got %d", operator.prepareCalled.Load())
	}
	if operator.beforeExecCalled.Load() != 1 {
		t.Errorf("beforeExecCalled should be 1, got %d", operator.beforeExecCalled.Load())
	}
}

func TestRunner_Prepare_OperatorPrepareError(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{prepareErr: errors.New("prepare fail")}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err == nil {
		t.Fatal("Prepare should return error")
	}
	if !contains(err.Error(), "operator.Prepare") {
		t.Errorf("error should contain 'operator.Prepare', got: %v", err)
	}
}

func TestRunner_Prepare_BeforeExecError(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{beforeExecErr: errors.New("before fail")}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err == nil {
		t.Fatal("Prepare should return error")
	}
	if !contains(err.Error(), "operator.BeforeExec") {
		t.Errorf("error should contain 'operator.BeforeExec', got: %v", err)
	}
}

func TestRunner_ReceiveAndStart(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{
		out:      []storage.MapEntry{{"key": "val"}},
		amount:   1,
		affected: 1,
	}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	r.Start()
	r.Receive([]storage.MapEntry{{"id": 1}})
	r.Done()
	err = r.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}

	out := <-r.OutChan()
	if len(out) != 1 {
		t.Errorf("expected 1 output item, got %d", len(out))
	}
	if executor.callCount.Load() != 1 {
		t.Errorf("executor.callCount should be 1, got %d", executor.callCount.Load())
	}
}

func TestRunner_Start_ExecError(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{err: errors.New("exec fail")}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, &mockOperator[storage.MapEntry]{})

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	r.Start()
	r.Receive([]storage.MapEntry{{"id": 1}})
	r.Done()
	err = r.Finish()
	if err == nil {
		t.Fatal("Finish should return error")
	}
	if !contains(err.Error(), "executor.Exec") {
		t.Errorf("error should contain 'executor.Exec', got: %v", err)
	}
}

func TestRunner_Finish_Success(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	r.Start()
	r.Done()
	err = r.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}

	if operator.afterExecCalled.Load() != 1 {
		t.Errorf("afterExecCalled should be 1, got %d", operator.afterExecCalled.Load())
	}

	// OutChan should be closed after Finish
	_, ok := <-r.OutChan()
	if ok {
		t.Error("OutChan should be closed after Finish")
	}
}

func TestRunner_Done(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	r.Start()
	r.Done()

	// inChan is closed; Finish should complete without error
	err = r.Finish()
	if err != nil {
		t.Fatalf("Finish should succeed, got: %v", err)
	}
}

func TestRunner_Finish_AfterExecError(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{afterExecErr: errors.New("after fail")}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	r.Start()
	r.Done()
	err = r.Finish()
	if err == nil {
		t.Fatal("Finish should return error")
	}
	if !contains(err.Error(), "operator.AfterExec") {
		t.Errorf("error should contain 'operator.AfterExec', got: %v", err)
	}
}

func TestRunner_Summary(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{summaryStr: "test-summary"}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	if r.Summary() != "test-summary" {
		t.Errorf("Summary should be 'test-summary', got '%s'", r.Summary())
	}
}

func TestRunner_State(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	state := r.State()
	if len(state) == 0 {
		t.Error("State should not be empty")
	}
	foundTotal := false
	for _, s := range state {
		if contains(s, "Total") {
			foundTotal = true
			break
		}
	}
	if !foundTotal {
		t.Errorf("State should contain 'Total', got: %v", state)
	}
}

func TestRunner_Output(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	ctx := context.Background()
	err := r.Prepare(ctx)
	if err != nil {
		t.Fatalf("Prepare should succeed, got: %v", err)
	}

	// Output returns nil when no output lines have been added; should not panic
	_ = r.Output()
}

func TestRunner_Close(t *testing.T) {
	executor := &mockExecutor[storage.MapEntry, storage.MapEntry]{}
	operator := &mockOperator[storage.MapEntry]{}
	r := NewRunner[storage.MapEntry, storage.MapEntry](executor, operator)

	err := r.Close()
	if err != nil {
		t.Fatalf("Close should succeed, got: %v", err)
	}
	if operator.closeCalled.Load() != 1 {
		t.Errorf("closeCalled should be 1, got %d", operator.closeCalled.Load())
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}