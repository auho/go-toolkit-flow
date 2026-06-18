package mysql

import (
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

type Gorm struct {
	DB    *gorm.DB
	SqlDB *sql.DB
}

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
