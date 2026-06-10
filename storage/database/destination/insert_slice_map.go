package destination

import (
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
)

var _ Destinationer[storage.MapEntry] = (*InsertSliceMap)(nil)

type InsertSliceMap struct {
}

func (i *InsertSliceMap) Exec(d *Destination[storage.MapEntry], items storage.MapEntries) error {
	return d.db.BulkInsertFromSliceMap(d.table, items, int(d.pageSize))
}

func NewInsertSliceMap(config *Config, b database.BuildDb) (*Destination[storage.MapEntry], error) {
	return NewDestination[storage.MapEntry](config, &InsertSliceMap{}, b)
}
