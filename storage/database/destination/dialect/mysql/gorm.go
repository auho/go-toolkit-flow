package mysql

import (
	"database/sql"
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// gormMySQL MySQL 方言实现
type gormMySQL struct {
	db     *gorm.DB
	sqlDB  *sql.DB
	config dialect.WriteConfig
}

// newGormMySQL 创建 MySQL 方言
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

// DBName 实现 Dialect 接口
func (g *gormMySQL) DBName() string {
	return g.db.Name()
}

// Ping 实现 Dialect 接口
func (g *gormMySQL) Ping() error {
	return g.sqlDB.Ping()
}

// Close 实现 Dialect 接口
func (g *gormMySQL) Close() error {
	return g.sqlDB.Close()
}

// Truncate 实现 Dialect 接口
func (g *gormMySQL) Truncate() error {
	return g.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", g.config.TableName)).Error
}

// BulkInsertMap 实现 Dialect 接口
// 迁移 simpledb.BulkInsertFromSliceMap 逻辑，使用 gorm 的 CreateInBatches
func (g *gormMySQL) BulkInsertMap(items storage.MapEntries, batchSize int) error {
	return g.db.Table(g.config.TableName).CreateInBatches(items, batchSize).Error
}

// BulkInsertSlice 实现 Dialect 接口
// 迁移 simpledb.BulkInsertFromSliceSlice 逻辑，先转为 map 再调用 BulkInsertMap
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

// BulkUpdateMap 实现 Dialect 接口
// 迁移 simpledb.BulkUpdateFromSliceMapById 逻辑
func (g *gormMySQL) BulkUpdateMap(idName string, items storage.MapEntries) error {
	for _, item := range items {
		_id, ok := item[idName]
		if !ok {
			return fmt.Errorf("table[%s] [%s] not found in map", g.config.TableName, idName)
		}

		err := g.db.Table(g.config.TableName).Where(fmt.Sprintf("%s = ?", idName), _id).UpdateColumns(item).Error
		if err != nil {
			return fmt.Errorf("table[%s] %s[%v] error %v", g.config.TableName, idName, _id, err)
		}
	}

	return nil
}
