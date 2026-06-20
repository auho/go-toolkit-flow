package source

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/source/format"
)

// NewMap creates a Memory source that generates MapEntry items.
func NewMap(config Config) *Memory[storage.MapEntry] {
	return NewMemory[storage.MapEntry](config, format.NewMapFormat())
}

// NewSlice creates a Memory source that generates SliceEntry items.
func NewSlice(config Config) *Memory[storage.SliceEntry] {
	return NewMemory[storage.SliceEntry](config, format.NewSliceFormat())
}

// NewString creates a Memory source that generates string items.
func NewString(config Config) *Memory[string] {
	return NewMemory[string](config, format.NewStringFormat())
}

// NewStringMap creates a Memory source that generates StringMapEntry items.
func NewStringMap(config Config) *Memory[storage.StringMapEntry] {
	return NewMemory[storage.StringMapEntry](config, format.NewStringMapFormat())
}
