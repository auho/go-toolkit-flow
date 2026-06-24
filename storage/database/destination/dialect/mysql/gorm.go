package mysql

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/client/mysql"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
)

// gormMySQL is the MySQL dialect implementation backed by gorm.
type gormMySQL struct {
	*mysql.Gorm

	config dialect.WriteConfig
}

// Truncate implements the Dialect interface.
func (g *gormMySQL) Truncate() error {
	return g.DB.Exec(fmt.Sprintf("TRUNCATE TABLE %s", g.config.TableName)).Error
}

// BulkInsertMap implements the Dialect interface.
func (g *gormMySQL) BulkInsertMap(items storage.MapEntries, batchSize int) error {
	return g.DB.Table(g.config.TableName).CreateInBatches(items, batchSize).Error
}

// BulkInsertSlice implements the Dialect interface.
func (g *gormMySQL) BulkInsertSlice(fields []string, items storage.SliceEntries, batchSize int) error {
	fieldsLen := len(fields)
	sm := make(storage.MapEntries, 0, len(items))
	for _, item := range items {
		m := make(map[string]any, fieldsLen)
		for k1, field := range fields {
			m[field] = item[k1]
		}

		sm = append(sm, m)
	}

	return g.BulkInsertMap(sm, batchSize)
}

// BulkUpdateMap implements the Dialect interface.
func (g *gormMySQL) BulkUpdateMap(idName string, items storage.MapEntries) error {
	for _, item := range items {
		_id, ok := item[idName]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", g.config.TableName, idName)
		}

		err := g.DB.Table(g.config.TableName).Where(fmt.Sprintf("`%s` = ?", idName), _id).Omit(idName).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", g.config.TableName, idName, _id, err)
		}
	}

	return nil
}
