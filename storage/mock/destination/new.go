package destination

import (
	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/mock/destination/format"
)

// NewInsertMap creates a Memory destination that receives MapEntry items
// in insert mode.
func NewInsertMap() (*Memory[storage.MapEntry], error) {
	return NewMemory[storage.MapEntry](format.NewInsertMapFormat()), nil
}

// NewInsertSlice creates a Memory destination that receives SliceEntry items
// in insert mode.
func NewInsertSlice() (*Memory[storage.SliceEntry], error) {
	return NewMemory[storage.SliceEntry](format.NewInsertSliceFormat()), nil
}

// NewUpdateMap creates a Memory destination that receives MapEntry items
// in update mode.
func NewUpdateMap() (*Memory[storage.MapEntry], error) {
	return NewMemory[storage.MapEntry](format.NewUpdateMapFormat()), nil
}
