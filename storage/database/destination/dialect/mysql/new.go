package mysql

import (
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// NewDialectGorm 创建基于 gorm 的 MySQL 方言
func NewDialectGorm(db *gorm.DB, config dialect.WriteConfig) (dialect.Dialect, error) {
	return newGormMySQL(db, config)
}
