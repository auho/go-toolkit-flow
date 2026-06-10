package source

import (
	"fmt"
	"strings"

	"github.com/auho/go-toolkit-flow/storage/database"
	"gorm.io/gorm"
)

type Config struct {
	Concurrency int
	Maximum     int64
	StartId     int64
	EndId       int64
	PageSize    int64
	TableName   string
	IdName      string
}

type QueryConfig struct {
	Config
	Fields []string
	Where  string // "field1 = ? and field2 = ?"
	Order  string // "field1 desc"
}

func (q *QueryConfig) querior(db *database.DB) *gorm.DB {
	tx := db.Table(q.TableName)
	if len(q.Fields) > 0 {
		var _s1 []string
		for _, _f := range q.Fields {
			_s1 = append(_s1, fmt.Sprintf("`%s`", _f))
		}

		tx = tx.Select(strings.Join(_s1, ","))
	}

	if q.Where != "" {
		tx = tx.Where(q.Where)
	}

	if q.Order != "" {
		tx = tx.Order(q.Order)
	}

	return tx
}
