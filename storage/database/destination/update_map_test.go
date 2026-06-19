package destination

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

var ussItemsChan = make(chan storage.MapEntries)
var uss *Bulk[storage.MapEntry]

func TestBilkUpdateMapFormatGorm(t *testing.T) {
	var err error
	uss, err = NewBulkUpdateMapWithGorm(BulkConfig{
		IsTruncate:  true,
		Concurrency: 4,
		PageSize:    7,
	}, WriteConfig{
		TableName: tableName,
	}, idName, gormDB)

	if err != nil {
		log.Fatal(err)
	}

	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 10)

	go _buildDataForUpdateMap(t, page, pageSize)

	err = uss.Prepare(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	uss.Accept()

	for items := range ussItemsChan {
		err = uss.Receive(items)
		if err != nil {
			log.Fatal(err)
		}
	}

	uss.Done()

	err = uss.Finish()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(uss.Summary())
	fmt.Println(uss.State())

	if uss.state.Amount() != page*pageSize {
		t.Error(fmt.Sprintf("actual != expected %d != %d", uss.state.Amount(), page*pageSize))
	}

	var dbAmount int64
	err = gormDB.Table(tableName).Count(&dbAmount).Error
	if err != nil {
		t.Error(err)
	}

	if uss.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", uss.state.Amount(), dbAmount))
	}
}

func _buildDataForUpdateMap(t *testing.T, page, pageSize int64) {
	var err error
	for i := int64(0); i < page; i++ {
		rows := make([][]any, pageSize)
		for j := int64(0); j < pageSize; j++ {
			rows[j] = []any{
				fmt.Sprintf("name-%d-%d", i, j),
				1,
			}
		}

		err = simpleDB.BulkInsertFromSliceSlice(tableName, []string{"name", "value"}, rows, 100)
		if err != nil {
			t.Error(err)
		}
	}

	for k := int64(0); k < page*pageSize; k += pageSize {
		var rows []map[string]any
		err = gormDB.Table(tableName).
			Select([]string{"id", "name", "value"}).
			Where(fmt.Sprintf("%s > ?", idName), k).
			Order(fmt.Sprintf("%s asc", idName)).
			Limit(int(pageSize)).
			Scan(&rows).Error
		if err != nil {
			t.Error(err)
		}

		for index, v := range rows {
			v[valueName] = 2
			rows[index] = v
		}

		ussItemsChan <- rows
	}

	close(ussItemsChan)
}
