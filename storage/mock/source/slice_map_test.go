package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestSliceMap(t *testing.T) {
	_testMock[storage.MapEntry](t, func(config Config) *Mock[storage.MapEntry] {
		return NewSliceMap(config)
	})
}
