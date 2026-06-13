package destination

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	goSimpleDb "github.com/auho/go-simple-db/v2"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
	"github.com/auho/go-toolkit-flow/tests/mysql"
)

var ussItemsChan = make(chan storage.MapEntries)
var uss *Destination[storage.MapEntry]

func TestUpdateSliceMap(t *testing.T) {
	var err error
	uss, err = NewUpdateSliceMapWithGorm(DestinationConfig{
		IsTruncate:  true,
		Concurrency: 4,
	}, WriteConfig{
		TableName: tableName,
		PageSize:  7,
	}, idName, mysql.DB.GormDB())

	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())
	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 10)

	go _buildDataForUpdateSliceMap(t, page, pageSize)

	err = uss.Accept()
	if err != nil {
		log.Fatal(err)
	}

	for items := range ussItemsChan {
		uss.Receive(items)
	}

	uss.Done()

	uss.Finish()

	fmt.Println(uss.Summary())
	fmt.Println(uss.State())

	if uss.state.Amount() != page*pageSize {
		t.Error(fmt.Sprintf("actual != expected %d != %d", uss.state.Amount(), page*pageSize))
	}

	var dbAmount int64
	err = uss.DB().Table(tableName).Count(&dbAmount).Error
	if err != nil {
		t.Error(err)
	}

	if uss.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", uss.state.Amount(), dbAmount))
	}
}

func _buildDataForUpdateSliceMap(t *testing.T, page, pageSize int64) {
	d, err := database.BuildDB(func() (*goSimpleDb.SimpleDB, error) {
		return goSimpleDb.NewMysql(mysqlDsn)
	})
	if err != nil {
		t.Error(err)
	}

	for i := int64(0); i < page; i++ {
		rows := make([][]any, pageSize, pageSize)
		for j := int64(0); j < pageSize; j++ {
			rows[j] = []any{
				fmt.Sprintf("name-%d-%d", i, j),
				1,
			}
		}

		err = d.BulkInsertFromSliceSlice(tableName, []string{"name", "value"}, rows, 100)
		if err != nil {
			t.Error(err)
		}
	}

	for k := int64(0); k < page*pageSize; k += pageSize {
		var rows []map[string]any
		err = d.Table(tableName).
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
