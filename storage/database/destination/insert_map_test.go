package destination

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestBulkInsertSliceMapGorm(t *testing.T) {
	iss, err := NewBulkInsertMapWithGorm(BulkConfig{
		IsTruncate:  true,
		Concurrency: 4,
		PageSize:    337,
	}, WriteConfig{
		TableName: tableName,
	}, gormDB)

	if err != nil {
		log.Fatal(err)
	}

	err = iss.Prepare(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	iss.Accept()

	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 1000)

	go func() {
		for i := int64(0); i < page; i++ {
			data := make([]map[string]any, pageSize)
			for j := int64(0); j < pageSize; j++ {
				data[j] = map[string]any{
					nameName:  fmt.Sprintf("name-%d-%d", i, j),
					valueName: i * j,
				}
			}

			err1 := iss.Receive(data)
			if err1 != nil {
				log.Fatal(err1)
			}
		}

		iss.Done()
	}()

	err = iss.Finish()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(iss.Summary())
	fmt.Println(iss.State())

	if iss.state.Amount() != page*pageSize {
		t.Error(fmt.Sprintf("actual != expected %d != %d", iss.state.Amount(), page*pageSize))
	}

	var dbAmount int64
	err = gormDB.Table(tableName).Count(&dbAmount).Error
	if err != nil {
		t.Error("db amount ", err)
	}

	if iss.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", iss.state.Amount(), dbAmount))
	}
}
