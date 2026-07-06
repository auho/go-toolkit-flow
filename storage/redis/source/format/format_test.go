package format

import (
	"errors"
	"testing"
)

func TestKeyFormat_Check_EmptyKey(t *testing.T) {
	f := &keyFormat{}
	err := f.Check()
	if !errors.Is(err, ErrKeyEmpty) {
		t.Errorf("expected ErrKeyEmpty, got %v", err)
	}
}
