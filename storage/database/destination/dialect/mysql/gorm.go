package mysql

import (
	"database/sql"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// gormMySQL is the MySQL dialect implementation backed by gorm.
type gormMySQL struct {
	db     *gorm.DB
	sqlDB  *sql.DB
	config dialect.WriteConfig
}

// newGormMySQL creates a MySQL dialect.
func newGormMySQL(db *gorm.DB, config dialect.WriteConfig) (*gormMySQL, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &gormMySQL{db: db, sqlDB: sqlDB, config: config}, nil
}

// DBName implements the Dialect interface.
func (g *gormMySQL) DBName() string {
	return g.db.Name()
}

// Ping implements the Dialect interface.
func (g *gormMySQL) Ping() error {
	return g.sqlDB.Ping()
}

// Close implements the Dialect interface.
func (g *gormMySQL) Close() error {
	return g.sqlDB.Close()
}

// Truncate implements the Dialect interface.
func (g *gormMySQL) Truncate() error {
	return g.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", g.config.TableName)).Error
}

// BulkInsertMap implements the Dialect interface.
func (g *gormMySQL) BulkInsertMap(items storage.MapEntries, batchSize int) error {
	return g.db.Table(g.config.TableName).CreateInBatches(items, batchSize).Error
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

		err := g.db.Table(g.config.TableName).Where(fmt.Sprintf("`%s` = ?", idName), _id).Omit(idName).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", g.config.TableName, idName, _id, err)
		}
	}

	return nil
}
