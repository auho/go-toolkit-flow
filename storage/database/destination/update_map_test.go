package destination

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
	"github.com/auho/go-toolkit-flow/storage"
)

var updateItemsChan = make(chan storage.MapEntries)
var updateBulk *Bulk[storage.MapEntry]

func TestBulkUpdateMapFormatGorm(t *testing.T) {
	var err error
	updateBulk, err = NewBulkUpdateMapWithGorm(BulkConfig{
		IsTruncate:  true,
		Concurrency: 4,
		PageSize:    7,
	}, WriteConfig{
		TableName: updateMapTable,
	}, idName, gormDB)

	if err != nil {
		log.Fatal(err)
	}

	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 10)

	go _buildDataForUpdateMap(t, page, pageSize)

	err = updateBulk.Prepare(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	updateBulk.Accept()

	for items := range updateItemsChan {
		err = updateBulk.Receive(items)
		if err != nil {
			log.Fatal(err)
		}
	}

	updateBulk.Done()

	err = updateBulk.Finish()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(updateBulk.Summary())
	fmt.Println(updateBulk.StateString())

	if updateBulk.state.Amount() != page*pageSize {
		t.Error(fmt.Sprintf("actual != expected %d != %d", updateBulk.state.Amount(), page*pageSize))
	}

	var dbAmount int64
	err = gormDB.Table(updateMapTable).Count(&dbAmount).Error
	if err != nil {
		t.Error(err)
	}

	if updateBulk.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", updateBulk.state.Amount(), dbAmount))
	}

	mysql.CleanData(updateMapTable)
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

		err = simpleDB.BulkInsertFromSliceSlice(updateMapTable, []string{"name", "value"}, rows, 100)
		if err != nil {
			t.Error(err)
		}
	}

	for k := int64(0); k < page*pageSize; k += pageSize {
		var rows []map[string]any
		err = gormDB.Table(updateMapTable).
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

		updateItemsChan <- rows
	}

	close(updateItemsChan)
}
