package destination

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
)

func TestBulkInsertSliceGorm(t *testing.T) {
	bulk, err := NewBulkInsertSliceWithGorm(
		BulkConfig{
			IsTruncate:  true,
			Concurrency: 4,
			PageSize:    337,
		},
		WriteConfig{
			TableName: insertSliceTable,
		},
		[]string{nameName, valueName},
		gormDB,
	)

	if err != nil {
		log.Fatal(err)
	}

	err = bulk.Prepare(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	bulk.Accept()

	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 1000)

	go func() {
		for i := int64(0); i < page; i++ {
			data := make([][]any, pageSize)
			for j := int64(0); j < pageSize; j++ {
				data[j] = []any{
					fmt.Sprintf("name-%d-%d", i, j),
					i * j,
				}
			}

			err1 := bulk.Receive(data)
			if err1 != nil {
				log.Fatal(err1)
			}
		}

		bulk.Done()
	}()

	err = bulk.Finish()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(bulk.Summary())
	fmt.Println(bulk.StateString())

	if bulk.state.Amount() != page*pageSize {
		t.Error(fmt.Sprintf("actual != expected %d != %d", bulk.state.Amount(), page*pageSize))
	}

	var dbAmount int64
	err = gormDB.Table(insertSliceTable).Count(&dbAmount).Error
	if err != nil {
		t.Error("db amount ", err)
	}

	if bulk.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", bulk.state.Amount(), dbAmount))
	}

	mysql.CleanData(simpleDB, insertSliceTable)
}
