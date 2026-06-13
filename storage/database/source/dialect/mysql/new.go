package mysql

import (
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect"
	"gorm.io/gorm"
)

func NewDialectGorm(config dialect.ScanConfig, db *gorm.DB) (dialect.Dialect, error) {
	return newGormMySQL(config, db)
}
