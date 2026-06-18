package source

import (
	"fmt"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source/dialect/mysql"
	"github.com/auho/go-toolkit-flow/storage/database/source/format"
	"gorm.io/gorm"
)

// NewSectionMapWithGorm creates a Section that reads MapEntry data from a MySQL
// database via gorm, using the given SectionConfig and ScanConfig.
func NewSectionMapWithGorm(c SectionConfig, sc ScanConfig, db *gorm.DB) (*Section[storage.MapEntry], error) {
	d, err := mysql.NewDialectGorm(sc, db)
	if err != nil {
		return nil, fmt.Errorf("NewDialectGorm: %w", err)
	}

	return newSection[storage.MapEntry](format.NewMapFormat(), d, c), nil
}
