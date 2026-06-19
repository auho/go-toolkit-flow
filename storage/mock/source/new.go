package source

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/source/format"
)

// NewMap creates a Memory source that generates MapEntry items.
func NewMap(config Config) *Memory[storage.MapEntry] {
	return NewMemory[storage.MapEntry](config, format.NewMapFormat())
}

// NewString creates a Memory source that generates string items.
func NewString(config Config) *Memory[string] {
	return NewMemory[string](config, format.NewStringFormat())
}

// NewMapOfString creates a Memory source that generates MapOfStringsEntry items.
func NewMapOfString(config Config) *Memory[storage.MapOfStringsEntry] {
	return NewMemory[storage.MapOfStringsEntry](config, format.NewMapOfStringFormat())
}
