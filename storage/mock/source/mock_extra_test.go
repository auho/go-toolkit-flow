package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

func TestMemory_Copy(t *testing.T) {
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

func TestMemory_Close(t *testing.T) {
	m := NewMap(Config{Total: 10, PageSize: 5})
	err := m.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestMemory_ConfigDefaults(t *testing.T) {
	m := NewMap(Config{})

	if m.state.Total() != 100 {
		t.Errorf("default total = %d, want 100", m.state.Total())
	}
	if m.state.PageSize() != 10 {
		t.Errorf("default pageSize = %d, want 10", m.state.PageSize())
	}
	if m.concurrency != 1 {
		t.Errorf("default concurrency = %d, want 1", m.concurrency)
	}
	if m.idName != "id" {
		t.Errorf("default idName = %q, want %q", m.idName, "id")
	}
}

func TestMemory_SummaryContent(t *testing.T) {
	m := NewMap(Config{Total: 50, PageSize: 10})
	summary := m.Summary()
	if len(summary) == 0 {
		t.Fatal("Summary() returned empty slice")
	}
}

func TestMemory_StateContent(t *testing.T) {
	m := NewMap(Config{Total: 50, PageSize: 10})
	overview := m.StateString()
	if len(overview) <= 0 {
		t.Fatal("StateString() returned empty string")
	}
}
