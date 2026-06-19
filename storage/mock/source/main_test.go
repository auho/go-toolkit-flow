package source

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
)

func _testMemory[E storage.Entry](t *testing.T, buildMemory func(Config) *Memory[E]) {
	factor := rand.Intn(10) + 1
	total := factor * 100
	pageSize := factor*factor + 1
	m := buildMemory(Config{
		PageSize: int64(pageSize),
		Total:    int64(total),
	})

	err := m.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	m.Scan()

	var finishErr error
	go func() {
		finishErr = m.Finish()
	}()

	amount := 0
	for items := range m.ReceiveChan() {
		amount = amount + len(items)
	}

	if finishErr != nil {
		t.Fatal(finishErr)
	}

	fmt.Println(m.Summary())
	fmt.Println(m.StateInfo().Overview())

	if m.StateInfo().Amount() != int64(amount) {
		t.Error(" amount ")
	}
}
