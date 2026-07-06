package mysql

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage/database/client/mysql"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// NewDialectGorm creates a MySQL dialect backed by gorm.
func NewDialectGorm(db *gorm.DB, config dialect.WriteConfig) (dialect.Dialect, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid write config: %w", err)
	}

	gormDB, err := mysql.NewGorm(db)
	if err != nil {
		return nil, fmt.Errorf("NewGorm: %w", err)
	}

	return &gormMySQL{Gorm: gormDB, config: config}, nil
}
