package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/source/format"
	"gorm.io/gorm"
)

func NewSectionMapWithGorm(c SectionConfig, sc ScanConfig, db *gorm.DB) (*Section[storage.MapEntry], error) {
	d, err := mysql.NewDialectGorm(sc, db)
	if err != nil {
		return nil, fmt.Errorf("NewGorm failed to create dialect: %w", err)
	}

	return newSection[storage.MapEntry](format.NewMapFormat(), d, c), nil
}
