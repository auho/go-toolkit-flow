package destination

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/mock/destination/format"
)

// NewInsertMap creates a Destination that receives MapEntry items
// in insert mode.
func NewInsertMap() (*Destination[storage.MapEntry], error) {
	return NewDestination[storage.MapEntry](format.NewInsertMapFormat()), nil
}

// NewInsertSlice creates a Destination that receives SliceEntry items
// in insert mode.
func NewInsertSlice() (*Destination[storage.SliceEntry], error) {
	return NewDestination[storage.SliceEntry](format.NewInsertSliceFormat()), nil
}

// NewUpdateMap creates a Destination that receives MapEntry items
// in update mode.
func NewUpdateMap() (*Destination[storage.MapEntry], error) {
	return NewDestination[storage.MapEntry](format.NewUpdateMapFormat()), nil
}