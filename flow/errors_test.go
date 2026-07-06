package flow

import (
	"errors"
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

func TestRunFlow_ErrSourceNotFound(t *testing.T) {
	err := RunFlow[storage.MapEntry, storage.MapEntry]()
	if !errors.Is(err, ErrSourceNotFound) {
		t.Errorf("expected ErrSourceNotFound, got %v", err)
	}
}

func TestRunFlow_ErrGroupNotFound(t *testing.T) {
	src := newFaultSource(faultSourceConfig{})
	err := RunFlow[storage.MapEntry, storage.MapEntry](
		WithSource[storage.MapEntry, storage.MapEntry](src),
	)
	if !errors.Is(err, ErrGroupNotFound) {
		t.Errorf("expected ErrGroupNotFound, got %v", err)
	}
}
