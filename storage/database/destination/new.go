package destination

import (
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	dialectMysql "github.com/auho/go-toolkit-flow/storage/database/destination/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/destination/format"
	"gorm.io/gorm"
)

// NewInsertSliceMapWithGorm 创建基于 gorm 的 MapEntry 插入 Destination
func NewInsertSliceMapWithGorm(config DestinationConfig, writeConfig WriteConfig, db *gorm.DB) (*Destination[storage.MapEntry], error) {
	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.MapEntry](config, writeConfig, d, format.NewInsertMapFormat(int(writeConfig.PageSize)))
}

// NewInsertSliceSliceWithGorm 创建基于 gorm 的 SliceEntry 插入 Destination
func NewInsertSliceSliceWithGorm(config DestinationConfig, writeConfig WriteConfig, fields []string, db *gorm.DB) (*Destination[storage.SliceEntry], error) {
	if len(fields) <= 0 {
		return nil, errors.New("fields is error")
	}

	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.SliceEntry](config, writeConfig, d, format.NewInsertSliceFormat(fields, int(writeConfig.PageSize)))
}

// NewUpdateSliceMapWithGorm 创建基于 gorm 的 MapEntry 更新 Destination
func NewUpdateSliceMapWithGorm(config DestinationConfig, writeConfig WriteConfig, idName string, db *gorm.DB) (*Destination[storage.MapEntry], error) {
	d, err := dialectMysql.NewDialectGorm(writeConfig, db)
	if err != nil {
		return nil, err
	}

	return newDestination[storage.MapEntry](config, writeConfig, d, format.NewUpdateMapFormat(idName))
}
