package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/source/format"
	"gorm.io/gorm"
)

func NewSectionMapWithGorm(config SectionConfig, scanConfig ScanConfig, db *gorm.DB) (*Section[storage.MapEntry], error) {
	dialect, err := mysql.NewDialectGorm(scanConfig, db)
	if err != nil {
		return nil, fmt.Errorf("NewGorm failed to create dialect: %w", err)
	}

	return newSection[storage.MapEntry](config, dialect, format.NewMapFormat()), nil
}
