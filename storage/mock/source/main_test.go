package source

import (
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

	err := m.Scan()
	if err != nil {
		t.Fatal(err)
	}

	amount := 0
	for items := range m.ReceiveChan() {
		amount = amount + len(items)
	}

	fmt.Println(m.Summary())
	fmt.Println(m.State())

	if m.amount != int64(amount) {
		t.Error(" amount ")
	}
}
