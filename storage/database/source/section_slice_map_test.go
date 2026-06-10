package source

import (
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/tests/mysql"
)

func TestSectionSliceMapFromTable(t *testing.T) {
	s, err := NewSectionSliceMap(
		&QueryConfig{
			Config: Config{
				Concurrency: 4,
				Maximum:     100000,
				StartId:     0,
				EndId:       100000,
				PageSize:    337,
				TableName:   tableName,
				IdName:      idName,
			},
			Fields: []string{nameName, valueName},
		}, func() (*database.DB, error) {
			return mysql.DB, nil
		})

	if err != nil {
		t.Error(err)
	}

	_testSection[storage.MapEntry](t, s)
}
