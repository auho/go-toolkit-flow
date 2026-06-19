package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func TestMapOfString(t *testing.T) {
	_testMemory[storage.MapOfStringsEntry](t, func(config Config) *Memory[storage.MapOfStringsEntry] {
		return NewMapOfString(config)
	})
}