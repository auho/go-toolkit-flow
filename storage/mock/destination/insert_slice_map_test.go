package destination

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestInsertSliceMap(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	page = rand.Intn(49) + 1
	pageSize = (rand.Intn(9) + 1) * pageSize

	d, err := NewInsertSliceMap()
	if err != nil {
		t.Error(err)
	}

	err = d.Accept()
	if err != nil {
		t.Error(err)
	}

	go func() {
		for i := 0; i < page; i++ {
			var sliceMap []map[string]any
			for j := 0; j < pageSize; j++ {
				m := make(map[string]any)
				m[idName] = i*page + j
				sliceMap = append(sliceMap, m)
			}

			d.Receive(sliceMap)
		}

		d.Done()
	}()

	d.Finish()

	fmt.Printf("page: %d, pageSize: %d, amount: %d \n", page, pageSize, page*pageSize)
	fmt.Println(d.Summary())
	fmt.Println(d.State())

	if d.amount != int64(page*pageSize) {
		t.Error(" amount ")
	}
}
