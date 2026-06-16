package destination

import (
	"errors"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect"
	"github.com/auho/go-toolkit-flow/storage/database/destination/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/destination/format"
	"gorm.io/gorm"
)

// NewBulkInsertMapWithGorm 创建基于 gorm 的 MapEntry 插入 Bulk
func NewBulkInsertMapWithGorm(c BulkConfig, wc WriteConfig, db *gorm.DB) (*Bulk[storage.MapEntry], error) {
	return newBulkWithGorm(format.NewInsertMapFormat(int(c.PageSize)), db, c, wc)
}

// NewBulkInsertSliceWithGorm 创建基于 gorm 的 SliceEntry 插入 Bulk
func NewBulkInsertSliceWithGorm(c BulkConfig, wc WriteConfig, fields []string, db *gorm.DB) (*Bulk[storage.SliceEntry], error) {
	if len(fields) <= 0 {
		return nil, errors.New("fields is error")
	}

	return newBulkWithGorm(format.NewInsertSliceFormat(fields, int(c.PageSize)), db, c, wc)
}

// NewBulkUpdateMapWithGorm 创建基于 gorm 的 MapEntry 更新 Bulk
func NewBulkUpdateMapWithGorm(c BulkConfig, wc WriteConfig, idName string, db *gorm.DB) (*Bulk[storage.MapEntry], error) {
	return newBulkWithGorm(format.NewUpdateMapFormat(idName), db, c, wc)
}

func newBulkWithGorm[E storage.Entry](f format.Format[E], db *gorm.DB, c BulkConfig, wc WriteConfig) (*Bulk[E], error) {
	d, err := newGorm(db, wc)
	if err != nil {
		return nil, err
	}

	return newBulk(f, d, c)
}

func newGorm(db *gorm.DB, wc WriteConfig) (dialect.Dialect, error) {
	return mysql.NewDialectGorm(db, wc)
}
