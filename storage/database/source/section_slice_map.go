package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/tool"
)

var _ sectionQuery[storage.MapEntry] = (*sectionSliceMap)(nil)

type sectionSliceMap struct{}

func (ssm *sectionSliceMap) Query(se *Section[storage.MapEntry], startId, endId int64) ([]storage.MapEntry, error) {
	var rows storage.MapEntries

	tx := se.conf.querior(se.db)
	err := tx.Where(fmt.Sprintf("%s >= ? and %s <= ?", se.conf.IdName, se.conf.IdName), startId, endId).
		Scan(&rows).Error

	return rows, err
}

func (ssm *sectionSliceMap) Copy(items storage.MapEntries) storage.MapEntries {
	return tool.CopySliceMap[tool.AnyEntry](items)
}

func NewSectionSliceMap(config *QueryConfig, newDb database.BuildDb) (*Section[storage.MapEntry], error) {
	return newSection[storage.MapEntry](config, &sectionSliceMap{}, newDb)
}
