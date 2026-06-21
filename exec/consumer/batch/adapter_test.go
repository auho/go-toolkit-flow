package batch

import (
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

type mockBatch struct{}

func (m *mockBatch) Summary() string   { return "mock" }
func (m *mockBatch) Prepare() error    { return nil }
func (m *mockBatch) BeforeRun() error  { return nil }
func (m *mockBatch) AfterRun() error   { return nil }
func (m *mockBatch) Close() error      { return nil }
func (m *mockBatch) AppendState()      {}
func (m *mockBatch) Concurrency() int  { return 1 }
func (m *mockBatch) Init()             {}
func (m *mockBatch) State() []string   { return nil }
func (m *mockBatch) Output() []string  { return nil }
func (m *mockBatch) Exec(items []storage.MapEntry) (int64, error) {
	return int64(len(items)), nil
}

type mockBatchErr struct{}

func (m *mockBatchErr) Summary() string   { return "mock-err" }
func (m *mockBatchErr) Prepare() error    { return nil }
func (m *mockBatchErr) BeforeRun() error  { return nil }
func (m *mockBatchErr) AfterRun() error   { return nil }
func (m *mockBatchErr) Close() error      { return nil }
func (m *mockBatchErr) AppendState()      {}
func (m *mockBatchErr) Concurrency() int  { return 1 }
func (m *mockBatchErr) Init()             {}
func (m *mockBatchErr) State() []string   { return nil }
func (m *mockBatchErr) Output() []string  { return nil }
func (m *mockBatchErr) Exec(items []storage.MapEntry) (int64, error) {
	return 0, errors.New("batch err")
}

func TestAdapter_NewRunner(t *testing.T) {
	r := NewRunner[storage.MapEntry, storage.MapEntry](&mockBatch{})
	if r == nil {
		t.Error("NewRunner should not return nil")
	}
}

func TestAdapterExec_Success(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{batch: &mockBatch{}}
	out, amount, affected, err := a.Exec([]storage.MapEntry{{"id": 1}})

	if err != nil {
		t.Fatalf("Exec should succeed, got: %v", err)
	}
	if out != nil {
		t.Errorf("out should be nil, got %v", out)
	}
	if amount != 1 {
		t.Errorf("amount should be 1, got %d", amount)
	}
	if affected != 1 {
		t.Errorf("affected should be 1, got %d", affected)
	}
}

func TestAdapterExec_Error(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{batch: &mockBatchErr{}}
	_, _, _, err := a.Exec([]storage.MapEntry{{"id": 1}})

	if err == nil {
		t.Fatal("Exec should return error")
	}
	if !contains(err.Error(), "batch.Exec") {
		t.Errorf("error should contain 'batch.Exec', got: %v", err)
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
