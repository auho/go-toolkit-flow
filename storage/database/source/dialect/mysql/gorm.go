package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
	"gorm.io/gorm"
)

// gormMySQL MySQL 方言实现
type gormMySQL struct {
	db     *gorm.DB
	sqlDB  *sql.DB
	config dialect.ScanConfig
}

// newGormMySQL 创建 MySQL 方言
func newGormMySQL(config dialect.ScanConfig, db *gorm.DB) (*gormMySQL, error) {
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

// FetchIDBounds 实现 MapQuerier 接口，查询表的 ID 最小值和最大值边界
func (g *gormMySQL) FetchIDBounds() (int64, int64, error) {
	var row struct {
		Max int64
		Min int64
	}

	query := fmt.Sprintf("MAX(%s) AS max, MIN(%s) AS min", g.config.SegmentIDName, g.config.SegmentIDName)
	err := g.db.Table(g.config.TableName).Select(query).Scan(&row).Error
	if err != nil {
		return 0, 0, fmt.Errorf("fetch id bounds: %w", err)
	}

	return row.Min, row.Max, nil
}

// QueryMapByRange 实现 MapQuerier 接口，查询指定 ID 范围内的 MapEntry 数据
func (g *gormMySQL) QueryMapByRange(startID, endID int64) (storage.MapEntries, error) {
	var rows storage.MapEntries

	tx := g.buildSelectQuery()
	err := tx.Where(fmt.Sprintf("%s >= ? and %s <= ?", g.config.SegmentIDName, g.config.SegmentIDName), startID, endID).
		Scan(&rows).Error

	return rows, err
}

// DBName 实现 gormMySQL 接口
func (g *gormMySQL) DBName() string {
	return g.db.Name()
}

// Close 实现 gormMySQL 接口
func (g *gormMySQL) Close() error {
	return g.sqlDB.Close()
}

// buildSelectQuery 构建 SELECT 查询，使用 MySQL 反引号包裹字段名
func (g *gormMySQL) buildSelectQuery() *gorm.DB {
	tx := g.db.Table(g.config.TableName)
	if len(g.config.SelectFields) > 0 {
		var quotedFields []string
		for _, field := range g.config.SelectFields {
			quotedFields = append(quotedFields, fmt.Sprintf("`%s`", field))
		}

		tx = tx.Select(strings.Join(quotedFields, ","))
	}

	if g.config.Where != "" {
		if len(g.config.WhereArgs) > 0 {
			tx = tx.Where(g.config.Where, g.config.WhereArgs...)
		} else {
			tx = tx.Where(g.config.Where)
		}
	}

	if g.config.Order != "" {
		tx = tx.Order(g.config.Order)
	}

	return tx
}
