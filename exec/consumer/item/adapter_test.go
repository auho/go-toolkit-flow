package item

import (
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

type mockItem struct {
	ok  bool
	err error
}

func (m *mockItem) Summary() string  { return "mock" }
func (m *mockItem) Prepare() error   { return nil }
func (m *mockItem) BeforeExec() error { return nil }
func (m *mockItem) AfterExec() error { return nil }
func (m *mockItem) Close() error     { return nil }
func (m *mockItem) AppendState()     {}
func (m *mockItem) Concurrency() int { return 1 }
func (m *mockItem) Init()            {}
func (m *mockItem) State() []string  { return nil }
func (m *mockItem) Output() []string { return nil }
func (m *mockItem) Exec(item storage.MapEntry) (bool, error) {
	return m.ok, m.err
}

type mockItemErr struct {
	ok bool
}

func (m *mockItemErr) Summary() string  { return "mock-err" }
func (m *mockItemErr) Prepare() error   { return nil }
func (m *mockItemErr) BeforeExec() error { return nil }
func (m *mockItemErr) AfterExec() error { return nil }
func (m *mockItemErr) Close() error     { return nil }
func (m *mockItemErr) AppendState()     {}
func (m *mockItemErr) Concurrency() int { return 1 }
func (m *mockItemErr) Init()            {}
func (m *mockItemErr) State() []string  { return nil }
func (m *mockItemErr) Output() []string { return nil }
func (m *mockItemErr) Exec(item storage.MapEntry) (bool, error) {
	return m.ok, errors.New("item err")
}

func TestAdapterNewRunner(t *testing.T) {
	r := NewRunner[storage.MapEntry, storage.MapEntry](&mockItem{ok: true})
	if r == nil {
		t.Error("NewRunner should not return nil")
	}
}

func TestAdapterExec_Success(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItem{ok: true}}
	out, amount, affected, err := a.Exec([]storage.MapEntry{{"id": 1}, {"id": 2}})

	if err != nil {
		t.Fatalf("Exec should succeed, got: %v", err)
	}
	if out != nil {
		t.Errorf("out should be nil, got %v", out)
	}
	if amount != 2 {
		t.Errorf("amount should be 2, got %d", amount)
	}
	if affected != 0 {
		t.Errorf("affected should be 0, got %d", affected)
	}
}

func TestAdapterExec_NotOk(t *testing.T) {
	a := &adapter[storage.MapEntry, storage.MapEntry]{item: &mockItem{ok: false}}
	out, amount, affected, err := a.Exec([]storage.MapEntry{{"id": 1}, {"id": 2}})

	if err != nil {
		t.Fatalf("Exec should succeed, got: %v", err)
	}
	if out != nil {
		t.Errorf("out should be nil, got %v", out)
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