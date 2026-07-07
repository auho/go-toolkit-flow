// Package mysql provides the MySQL dialect implementation for the destination package.
package mysql

import (
	"context"
	"fmt"

	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/client/mysql"
	"github.com/auho/go-toolkit-flow/v3/storage/database/destination/dialect"
	"gorm.io/gorm"
)

// gormMySQL is the MySQL dialect implementation backed by gorm.
type gormMySQL struct {
	*mysql.Gorm

	config dialect.WriteConfig
}

// Truncate implements the Dialect interface.
func (g *gormMySQL) Truncate(ctx context.Context) error {
	err := g.DB.WithContext(ctx).Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", g.config.TableName)).Error
	if err != nil {
		return fmt.Errorf("Exec: table[%s]: %w", g.config.TableName, err)
	}
	return nil
}

// BulkInsertMap implements the Dialect interface.
func (g *gormMySQL) BulkInsertMap(ctx context.Context, items storage.MapEntries, batchSize int) error {
	err := g.DB.WithContext(ctx).Table(g.config.TableName).CreateInBatches(items, batchSize).Error
	if err != nil {
		return fmt.Errorf("CreateInBatches: table[%s]: %w", g.config.TableName, err)
	}
	return nil
}

// BulkInsertSlice implements the Dialect interface.
func (g *gormMySQL) BulkInsertSlice(ctx context.Context, fields []string, items storage.SliceEntries, batchSize int) error {
	fieldsLen := len(fields)
	sm := make(storage.MapEntries, 0, len(items))
	for _, item := range items {
		m := make(map[string]any, fieldsLen)
		for k1, field := range fields {
			m[field] = item[k1]
		}

		sm = append(sm, m)
	}

	return g.BulkInsertMap(ctx, sm, batchSize)
}

// updateTxBatchSize is the max number of UPDATE statements per transaction.
// items received by BulkUpdateMap are already chunked to BatchSize by bulk.go's
// buffer flush; this further splits them into smaller transaction batches to
// limit transaction size and lock duration.
const updateTxBatchSize = 100

// BulkUpdateMap implements the Dialect interface.
func (g *gormMySQL) BulkUpdateMap(ctx context.Context, idName string, items storage.MapEntries) error {
	for i := 0; i < len(items); i += updateTxBatchSize {
		end := i + updateTxBatchSize
		if end > len(items) {
			end = len(items)
		}
		batch := items[i:end]

		err := g.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			for _, item := range batch {
				_id, ok := item[idName]
				if !ok {
					return fmt.Errorf("table[%s] [%s] not found in map", g.config.TableName, idName)
				}

				err := tx.Table(g.config.TableName).Where(fmt.Sprintf("`%s` = ?", idName), _id).Omit(idName).UpdateColumns(item).Error
				if err != nil {
					return fmt.Errorf("UpdateColumns: table[%s] %s[%v]: %w", g.config.TableName, idName, _id, err)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}
