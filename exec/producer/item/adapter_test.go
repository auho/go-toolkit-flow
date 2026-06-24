package item

import (
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

type mockItem struct {
	out []storage.MapEntry
	ok  bool
	err error
}

func (m *mockItem) Summary() string  { return "mock" }
func (m *mockItem) Prepare() error   { return nil }
func (m *mockItem) BeforeRun() error { return nil }
func (m *mockItem) AfterRun() error  { return nil }
func (m *mockItem) Close() error     { return nil }
func (m *mockItem) AppendState()     {}
func (m *mockItem) Concurrency() int { return 1 }
func (m *mockItem) State() []string  { return nil }
func (m *mockItem) Output() []string { return nil }
func (m *mockItem) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	return m.out, m.ok, m.err
}

type mockItemErr struct{}

func (m *mockItemErr) Summary() string  { return "mock-err" }
func (m *mockItemErr) Prepare() error   { return nil }
func (m *mockItemErr) BeforeRun() error { return nil }
func (m *mockItemErr) AfterRun() error  { return nil }
func (m *mockItemErr) Close() error     { return nil }
func (m *mockItemErr) AppendState()     {}
func (m *mockItemErr) Concurrency() int { return 1 }
func (m *mockItemErr) State() []string  { return nil }
func (m *mockItemErr) Output() []string { return nil }
func (m *mockItemErr) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	return nil, false, errors.New("item err")
}

type mockItemAfterBatchErr struct{}

func (m *mockItemAfterBatchErr) Summary() string  { return "mock-ab-err" }
func (m *mockItemAfterBatchErr) Prepare() error   { return nil }
func (m *mockItemAfterBatchErr) BeforeRun() error { return nil }
func (m *mockItemAfterBatchErr) AfterRun() error  { return nil }
func (m *mockItemAfterBatchErr) Close() error     { return nil }
func (m *mockItemAfterBatchErr) AppendState()     {}
func (m *mockItemAfterBatchErr) Concurrency() int { return 1 }
func (m *mockItemAfterBatchErr) State() []string  { return nil }
func (m *mockItemAfterBatchErr) Output() []string { return nil }
func (m *mockItemAfterBatchErr) Exec(item storage.MapEntry) ([]storage.MapEntry, bool, error) {
	return []storage.MapEntry{{"key": "val"}}, true, nil
}

func (m *mockItemAfterBatchErr) AfterBatch(items []storage.MapEntry) error {
	return errors.New("after batch err")
}

func TestAdapter_NewRunner(t *testing.T) {
	r := NewRunner[storage.MapEntry, storage.MapEntry](&mockItem{ok: true})
	if r == nil {
		t.Error("NewRunner should not return nil")
	}
}

func TestAdapterExec_Success(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItem{
		out: []storage.MapEntry{{"key": "val"}},
		ok:  true,
	}}
	items := []storage.MapEntry{{"id": 1}}
	out, amount, affected, err := a.Exec(items)

	if err != nil {
		t.Fatalf("Exec should succeed, got: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("out should have 1 item, got %d", len(out))
	}
	if amount != 1 {
		t.Errorf("amount should be 1, got %d", amount)
	}
	if affected != 1 {
		t.Errorf("affected should be 1, got %d", affected)
	}
}

func TestAdapterExec_NotOk(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItem{ok: false}}
	items := []storage.MapEntry{{"id": 1}}
	out, amount, affected, err := a.Exec(items)

	if err != nil {
		t.Fatalf("Exec should succeed, got: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("out should be empty, got %v", out)
	}
	if amount != 0 {
		t.Errorf("amount should be 0, got %d", amount)
	}
	if affected != 0 {
		t.Errorf("affected should be 0, got %d", affected)
	}
}

func TestAdapterExec_Error(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItemErr{}}
	_, _, _, err := a.Exec([]storage.MapEntry{{"id": 1}})

	if err == nil {
		t.Fatal("Exec should return error")
	}
	if !contains(err.Error(), "item.Exec") {
		t.Errorf("error should contain 'item.Exec', got: %v", err)
	}
}

func TestAdapter_AfterBatch_Error(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItemAfterBatchErr{}}
	_, _, _, err := a.Exec([]storage.MapEntry{{"id": 1}})

	if err == nil {
		t.Fatal("Exec should return error")
	}
	if !contains(err.Error(), "item.AfterBatch") {
		t.Errorf("error should contain 'item.AfterBatch', got: %v", err)
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
