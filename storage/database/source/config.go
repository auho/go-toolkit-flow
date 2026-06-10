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
	StartID     int64
	EndID       int64
	PageSize    int64
	TableName   string
	IDName      string
}

type QueryConfig struct {
	Config
	Fields []string
	Where  string // "field1 = ? and field2 = ?"
	Order  string // "field1 desc"
}

func (q *QueryConfig) buildQuery(db *database.DB) *gorm.DB {
	tx := db.Table(q.TableName)
	if len(q.Fields) > 0 {
		var quotedFields []string
		for _, field := range q.Fields {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
		}

		tx = tx.Select(strings.Join(quotedFields, ","))
	}

	if q.Where != "" {
		tx = tx.Where(q.Where)
	}

	if q.Order != "" {
		tx = tx.Order(q.Order)
	}

	return tx
}
