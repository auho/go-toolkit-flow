package destination

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/destination/format"
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

	state := d.State()
	if len(state) != 1 {
		t.Fatalf("expected 1 state line, got %d", len(state))
	}

	expected := "amount: 1"
	if state[0] != expected {
		t.Errorf("expected %q, got %q", expected, state[0])
	}
}

func TestMemory_SummaryContent(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	summary := d.Summary()
	if len(summary) == 0 {
		t.Fatal("Summary() returned empty slice")
	}
}

func TestMemory_Amount_PrivateField(t *testing.T) {
	d := NewMemory(format.NewInsertMapFormat())
	d.Accept()
	_ = d.Receive([]storage.MapEntry{{"id": 1}, {"id": 2}})
	d.Done()
	_ = d.Finish()

	if d.amount != 2 {
		t.Errorf("amount = %d, want 2", d.amount)
	}
}
