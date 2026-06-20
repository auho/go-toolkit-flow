package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestStringMap(t *testing.T) {
	_testMemory[storage.StringMapEntry](t, func(config Config) *Memory[storage.StringMapEntry] {
		return NewStringMap(config)
	})
}
