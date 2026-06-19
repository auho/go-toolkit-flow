package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestMock_Copy(t *testing.T) {
	m := NewMap(Config{Total: 10, PageSize: 5})

	original := []storage.MapEntry{
		{"id": 1, "name": "foo"},
		{"id": 2, "name": "bar"},
	}
	copied := m.Copy(original)

	if len(copied) != len(original) {
		t.Fatalf("Copy() length = %d, want %d", len(copied), len(original))
	}

	// Modify copy and verify original is unaffected
	copied[0]["name"] = "modified"
	if original[0]["name"] != "foo" {
		t.Error("modifying copy affected original slice")
	}
}

func TestMock_Close(t *testing.T) {
	m := NewMap(Config{Total: 10, PageSize: 5})
	err := m.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestMock_ConfigDefaults(t *testing.T) {
	m := NewMap(Config{})

	if m.total != 100 {
		t.Errorf("default total = %d, want 100", m.total)
	}
	if m.pageSize != 10 {
		t.Errorf("default pageSize = %d, want 10", m.pageSize)
	}
	if m.concurrency != 1 {
		t.Errorf("default concurrency = %d, want 1", m.concurrency)
	}
	if m.idName != "id" {
		t.Errorf("default idName = %q, want %q", m.idName, "id")
	}
}

func TestMock_SummaryContent(t *testing.T) {
	m := NewMap(Config{Total: 50, PageSize: 10})
	summary := m.Summary()
	if len(summary) == 0 {
		t.Fatal("Summary() returned empty slice")
	}
}

func TestMock_StateContent(t *testing.T) {
	m := NewMap(Config{Total: 50, PageSize: 10})
	state := m.State()
	if len(state) == 0 {
		t.Fatal("State() returned empty slice")
	}
}