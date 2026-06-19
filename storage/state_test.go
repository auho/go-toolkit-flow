package storage

import (
	"strings"
	"testing"
)

func TestState_New(t *testing.T) {
	s := NewStateInfo()
	if s == nil {
		t.Fatal("NewState() == nil")
	}
}

func TestState_Status(t *testing.T) {
	s := NewStateInfo()
	s.SetStatus("running")
	if s.Status() != "running" {
		t.Errorf("Status() = %q, want %q", s.Status(), "running")
	}
}

func TestState_MarkAsConfigured(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsConfigured()
	if s.Status() != StatusConfig {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusConfig)
	}
}

func TestState_MarkAsPrepare(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsPrepare()
	if s.Status() != StatusPrepare {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusPrepare)
	}
}

func TestState_MarkAsAccepted(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsAccepted()
	if s.Status() != StatusAccept {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusAccept)
	}
}

func TestState_MarkAsScanning(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsScanning()
	if s.Status() != StatusScan {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusScan)
	}
}

func TestState_MarkAsDone(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsDone()
	if s.Status() != StatusDone {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusDone)
	}
}

func TestState_MarkAsFinished(t *testing.T) {
	s := NewStateInfo()
	s.MarkAsFinished()
	if s.Status() != StatusFinish {
		t.Errorf("Status() = %q, want %q", s.Status(), StatusFinish)
	}
}

func TestState_Amount(t *testing.T) {
	s := NewStateInfo()
	s.SetAmount(100)
	if s.Amount() != 100 {
		t.Errorf("Amount() = %d, want %d", s.Amount(), 100)
	}

	s.AddAmount(50)
	if s.Amount() != 150 {
		t.Errorf("Amount() after AddAmount = %d, want %d", s.Amount(), 150)
	}
}

func TestState_Duration(t *testing.T) {
	s := NewStateInfo()
	s.DurationStart()
	s.DurationStop()
}

func TestTotalState_New(t *testing.T) {
	ts := NewTotalState()
	if ts == nil {
		t.Fatal("NewTotalState() == nil")
	}
}

func TestTotalState_Overview(t *testing.T) {
	ts := NewTotalState()
	ts.SetStatus(StatusDone)
	ts.SetAmount(50)
	ts.SetTotal(100)
	ts.SetConcurrency(2)

	overview := ts.Overview()
	if overview == "" {
		t.Fatal("Overview() returned empty string")
	}

	// The actual format: Status: %s, Concurrency: %d, Amount: %d/%d, Duration: %s
	// Total value (100) appears in "50/100" but not as a literal label
	labelChecks := []string{"Status", "Concurrency", "Amount", "Duration"}
	for _, c := range labelChecks {
		if !strings.Contains(overview, c) {
			t.Errorf("Overview() missing %q in %q", c, overview)
		}
	}

	if !strings.Contains(overview, "50/100") {
		t.Errorf("Overview() should contain Amount/Total value %q in %q", "50/100", overview)
	}
}

func TestPageState_New(t *testing.T) {
	ps := NewPageState()
	if ps == nil {
		t.Fatal("NewPageState() == nil")
	}
}

func TestPageState_AddPage(t *testing.T) {
	ps := NewPageState()
	ps.AddPage(1)
	if ps.Page() != 1 {
		t.Errorf("GetPage() = %d, want %d", ps.Page(), 1)
	}

	ps.AddPage(2)
	if ps.Page() != 3 {
		t.Errorf("Page() after AddPage(2) = %d, want %d", ps.Page(), 3)
	}
}

func TestPageState_Overview(t *testing.T) {
	ps := NewPageState()
	ps.SetStatus(StatusDone)
	ps.SetAmount(50)
	ps.SetTotal(100)
	ps.SetPageSize(10)
	ps.SetTotalPage(10)
	ps.AddPage(5)

	overview := ps.Overview()
	if overview == "" {
		t.Fatal("Overview() returned empty string")
	}

	// The actual format: Status: %s, Concurrency: %d, Amount: %d/%d, Page: %d/%d(%d), Duration: %s
	// PageSize/TotalPage/Total appear as numeric values, not as literal labels
	labelChecks := []string{"Status", "Amount", "Page", "Duration"}
	for _, c := range labelChecks {
		if !strings.Contains(overview, c) {
			t.Errorf("Overview() missing %q in %q", c, overview)
		}
	}

	if !strings.Contains(overview, "50/100") {
		t.Errorf("Overview() should contain Amount/Total value %q in %q", "50/100", overview)
	}
	if !strings.Contains(overview, "5/10") {
		t.Errorf("Overview() should contain Page/TotalPage value %q in %q", "5/10", overview)
	}
}
