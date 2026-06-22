package processor

import (
	"testing"
)

func TestBaseProcessor_State_Empty(t *testing.T) {
	var op BaseProcessor

	if s := op.State(); len(s) != 0 {
		t.Errorf("State() should be empty, got %v", s)
	}
}

func TestBaseProcessor_AddStateLine(t *testing.T) {
	var op BaseProcessor

	n1 := op.AddStateLine("line1")
	if n1 != 1 {
		t.Errorf("AddStateLine first call should return 1, got %d", n1)
	}

	n2 := op.AddStateLine("line2")
	if n2 != 2 {
		t.Errorf("AddStateLine second call should return 2, got %d", n2)
	}

	s := op.State()
	if len(s) != 2 {
		t.Fatalf("State() should have 2 lines, got %d", len(s))
	}
	if s[0] != "line1" {
		t.Errorf("State()[0] should be 'line1', got %q", s[0])
	}
	if s[1] != "line2" {
		t.Errorf("State()[1] should be 'line2', got %q", s[1])
	}
}

func TestBaseProcessor_SetStateLine(t *testing.T) {
	var op BaseProcessor

	n := op.AddStateLine("old")
	if n != 1 {
		t.Fatalf("AddStateLine should return 1, got %d", n)
	}

	op.SetStateLine(1, "new")

	s := op.State()
	if len(s) != 1 {
		t.Fatalf("State() should have 1 line, got %d", len(s))
	}
	if s[0] != "new" {
		t.Errorf("State()[0] should be 'new', got %q", s[0])
	}
}

func TestBaseProcessor_Output_Empty(t *testing.T) {
	var op BaseProcessor

	if o := op.Output(); len(o) != 0 {
		t.Errorf("Output() should be empty, got %v", o)
	}
}

func TestBaseProcessor_Outputln(t *testing.T) {
	var op BaseProcessor

	op.Outputln("hello")

	o := op.Output()
	if len(o) != 1 {
		t.Fatalf("Output() should have 1 line, got %d", len(o))
	}
	if o[0] != "hello" {
		t.Errorf("Output()[0] should be 'hello', got %q", o[0])
	}
}

func TestBaseProcessor_Outputf(t *testing.T) {
	var op BaseProcessor

	op.Outputf("hello %s", "world")

	o := op.Output()
	if len(o) != 1 {
		t.Fatalf("Output() should have 1 line, got %d", len(o))
	}
	if o[0] != "hello world" {
		t.Errorf("Output()[0] should be 'hello world', got %q", o[0])
	}
}

func TestBaseProcessor_Log_Empty(t *testing.T) {
	var op BaseProcessor

	if l := op.Log(); len(l) != 0 {
		t.Errorf("Log() should be empty, got %v", l)
	}
}

func TestBaseProcessor_Logln(t *testing.T) {
	var op BaseProcessor

	op.Logln("hello")

	l := op.Log()
	if len(l) != 1 {
		t.Fatalf("Log() should have 1 line, got %d", len(l))
	}
	if l[0] != "hello" {
		t.Errorf("Log()[0] should be 'hello', got %q", l[0])
	}
}

func TestBaseProcessor_Logf(t *testing.T) {
	var op BaseProcessor

	op.Logf("hello %s", "world")

	l := op.Log()
	if len(l) != 1 {
		t.Fatalf("Log() should have 1 line, got %d", len(l))
	}
	if l[0] != "hello world" {
		t.Errorf("Log()[0] should be 'hello world', got %q", l[0])
	}
}
