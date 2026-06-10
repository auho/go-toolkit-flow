package source

import (
	"fmt"
	"testing"
)

func TestNewScan(t *testing.T) {
	s, err := NewScan(Config{
		Concurrency: 1,
		Amount:      0,
		PageSize:    0,
		Key:         "",
		Options:     &_redisOptions,
	})

	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal("new", err)
	}

	err = s.Scan()
	if err != nil {
		t.Fatal("scan", err)
	}

	amount := 0
	for items := range s.ReceiveChan() {
		l := len(items)
		amount = amount + l
	}

	fmt.Println(s.Summary())
	fmt.Println(s.State())

	if s.state.Amount() != int64(amount) {
		t.Error(fmt.Sprintf("statusAmount != actual %d != %d", s.state.Amount(), amount))
	}

	err = s.Close()
	if err != nil {
		t.Error(err)
	}
}
