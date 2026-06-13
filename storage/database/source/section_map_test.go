package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tests/mysql"
)

func TestSectionSliceMapFromTable(t *testing.T) {
	s, err := NewSectionMapWithGorm(
		SectionConfig{
			Concurrency: 4,
			MaxItems:    100000,
			StartID:     0,
			EndID:       100000,
			PageSize:    337,
		},
		ScanConfig{
			TableName:     tableName,
			SegmentIDName: idName,
			SelectFields:  []string{nameName, valueName},
			WhereArgs:     nil,
		},
		mysql.DB.GormDB(),
	)

	if err != nil {
		t.Error(err)
	}

	_testSection[storage.MapEntry](t, s)
}
