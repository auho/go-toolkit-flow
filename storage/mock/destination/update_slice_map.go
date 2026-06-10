package destination

import (
	"github.com/auho/go-toolkit-flow/storage"
)

var _ storage.Destinationer[storage.MapEntry] = (*UpdateSliceMap)(nil)

type UpdateSliceMap struct {
	Destination[storage.MapEntry]
}

func NewUpdateSliceMap() (*UpdateSliceMap, error) {
	return &UpdateSliceMap{}, nil
}
