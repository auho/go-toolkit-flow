package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/v3/storage"
)

func TestMap(t *testing.T) {
	_testMemory[storage.MapEntry](t, func(config Config) *Memory[storage.MapEntry] {
		return NewMap(config)
	})
}
