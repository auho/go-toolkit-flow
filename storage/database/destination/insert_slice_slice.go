package destination

import (
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
)

var _ Executor[storage.SliceEntry] = (*InsertSliceSlice)(nil)

type InsertSliceSlice struct {
	fields []string
}

func (i *InsertSliceSlice) Exec(d *Destination[storage.SliceEntry], items storage.SliceEntries) error {
	return d.db.BulkInsertFromSliceSlice(d.table, i.fields, items, int(d.pageSize))
}

func NewInsertSliceSlice(config *Config, fields []string, b database.BuildDb) (*Destination[storage.SliceEntry], error) {
	if len(fields) <= 0 {
		return nil, errors.New("fields is error")
	}

	iss := &InsertSliceSlice{}
	iss.fields = fields

	return NewDestination[storage.SliceEntry](config, iss, b)
}
