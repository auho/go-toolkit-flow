package destination

import (
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	dialectMysql "github.com/auho/go-toolkit-flow/storage/database/destination/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/destination/format"
	"gorm.io/gorm"
)

// NewBulkInsertMapWithGorm 创建基于 gorm 的 MapEntry 插入 Bulk
func NewBulkInsertMapWithGorm(config BulkConfig, writeConfig WriteConfig, db *gorm.DB) (*Bulk[storage.MapEntry], error) {
	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.MapEntry](config, d, format.NewInsertMapFormat(int(config.PageSize)))
}

// NewBulkInsertSliceWithGorm 创建基于 gorm 的 SliceEntry 插入 Bulk
func NewBulkInsertSliceWithGorm(config BulkConfig, writeConfig WriteConfig, fields []string, db *gorm.DB) (*Bulk[storage.SliceEntry], error) {
	if len(fields) <= 0 {
		return nil, errors.New("fields is error")
	}

	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.SliceEntry](config, d, format.NewInsertSliceFormat(fields, int(config.PageSize)))
}

// NewBulkUpdateMapWithGorm 创建基于 gorm 的 MapEntry 更新 Bulk
func NewBulkUpdateMapWithGorm(config BulkConfig, writeConfig WriteConfig, idName string, db *gorm.DB) (*Bulk[storage.MapEntry], error) {
	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.MapEntry](config, d, format.NewUpdateMapFormat(idName))
}
