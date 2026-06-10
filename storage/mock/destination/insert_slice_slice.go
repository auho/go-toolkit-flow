package destination

import (
	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destination[storage.SliceEntry] = (*InsertSliceSlice)(nil)

type InsertSliceSlice struct {
	Destination[storage.SliceEntry]
}

func NewInsertSliceSlice() (*InsertSliceSlice, error) {
	return &InsertSliceSlice{}, nil
}
