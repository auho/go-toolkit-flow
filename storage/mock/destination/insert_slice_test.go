package destination

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func TestInsertSlice(t *testing.T) {
	page = rand.Intn(49) + 1
	pageSize = (rand.Intn(9) + 1) * pageSize

	d, err := NewInsertSlice()
	if err != nil {
		t.Error(err)
	}

	err = d.Prepare(context.Background())
	if err != nil {
		t.Error(err)
	}
	d.Accept()

	go func() {
		for i := 0; i < page; i++ {
			var sliceSlice [][]any
			for j := 0; j < pageSize; j++ {
				m := make([]any, 0)
				m = append(m, i*page+j)
				sliceSlice = append(sliceSlice, m)
			}

			d.Receive(sliceSlice)
		}

		d.Done()
	}()

	d.Finish()

	fmt.Printf("page: %d, pageSize: %d, amount: %d \n", page, pageSize, page*pageSize)
	fmt.Println(d.Summary())
	fmt.Println(d.StateInfo().Overview())

	if d.StateInfo().Amount() != int64(page*pageSize) {
		t.Error(" amount ")
	}
}