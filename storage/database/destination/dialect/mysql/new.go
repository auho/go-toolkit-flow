package mysql

import (
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// NewDialectGorm creates a MySQL dialect backed by gorm.
func NewDialectGorm(db *gorm.DB, config dialect.WriteConfig) (dialect.Dialect, error) {
	return newGormMySQL(db, config)
}
