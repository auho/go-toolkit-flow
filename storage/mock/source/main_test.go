package source

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
)

func _testMock[E storage.Entry](t *testing.T, buildMock func(Config) *Mock[E]) {
	rand.Seed(time.Now().UnixNano())
	factor := rand.Intn(10) + 1
	total := factor * 100
	pageSize := factor*factor + 1
	m := buildMock(Config{
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
	fmt.Println(m.State())

	if m.amount != int64(amount) {
		t.Error(" amount ")
	}
}
