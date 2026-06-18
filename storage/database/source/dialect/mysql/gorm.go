// Package mysql provides the MySQL dialect implementation for the source package.
package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
	"gorm.io/gorm"
)

// gormMySQL is the MySQL dialect implementation backed by gorm.
type gormMySQL struct {
	db     *gorm.DB
	sqlDB  *sql.DB
	config dialect.ScanConfig
}

// newGormMySQL creates a MySQL dialect backed by gorm.
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

// FetchIDBounds queries the minimum and maximum ID bounds of the table.
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

// QueryMapByRange queries MapEntry data within the given ID range.
func (g *gormMySQL) QueryMapByRange(startID, endID int64) (storage.MapEntries, error) {
	var rows storage.MapEntries

	tx := g.buildSelectQuery()
	err := tx.Where(fmt.Sprintf("`%s` >= ? and `%s` <= ?", g.config.SegmentIDName, g.config.SegmentIDName), startID, endID).
		Scan(&rows).Error

	return rows, err
}

// DBName returns the name of the underlying database.
func (g *gormMySQL) DBName() string {
	return g.db.Name()
}

// Close closes the underlying database connection.
func (g *gormMySQL) Close() error {
	return g.sqlDB.Close()
}

// buildSelectQuery builds a SELECT query with MySQL backtick-quoted field names.
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
