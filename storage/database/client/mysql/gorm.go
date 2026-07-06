// Package mysql provides a MySQL client wrapper for GORM.
package mysql

import (
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

// Gorm wraps a gorm.DB and its underlying *sql.DB.
type Gorm struct {
	// DB is the GORM ORM handle.
	DB *gorm.DB
	// SqlDB is the underlying database/sql handle for low-level operations.
	SqlDB *sql.DB
}

// NewGorm creates a Gorm from an existing gorm.DB, verifying connectivity with a Ping.
func NewGorm(db *gorm.DB) (*Gorm, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("db: %w", err)
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &Gorm{DB: db, SqlDB: sqlDB}, nil
}

// DBName implements the Dialect interface.
func (g *Gorm) DBName() string {
	return g.DB.Name()
}

// Close implements the Dialect interface.
func (g *Gorm) Close() error {
	return g.SqlDB.Close()
}
