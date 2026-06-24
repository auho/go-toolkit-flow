package destination

import (
	"strings"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/mock/destination/format"
)

func TestMemory_Done_Idempotent(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	d.Accept()
	d.Done()
	d.Done()
}

func TestMemory_Close(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	err := d.Close()
	if err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
}

func TestMemory_State_Format(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	d.Accept()
	_ = d.Receive([]storage.MapEntry{{"id": 1}})
	d.Done()
	_ = d.Finish()

	overview := d.StateString()
	if len(overview) <= 0 {
		t.Fatal("expected non-empty overview, got empty string")
	}

	if !strings.Contains(overview[0], "Amount: 1") {
		t.Errorf("expected overview to contain %q, got %q", "Amount: 1", overview)
	}
}

func TestMemory_SummaryContent(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	summary := d.Summary()
	if len(summary) == 0 {
		t.Fatal("Summary() returned empty slice")
	}
}

func TestMemory_Amount(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	d.Accept()
	_ = d.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	d.Done()
	_ = d.Finish()

	if d.StateInfo().Amount() != 2 {
		t.Errorf("amount = %d, want 2", d.StateInfo().Amount())
	}
}
