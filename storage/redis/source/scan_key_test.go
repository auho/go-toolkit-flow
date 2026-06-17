package source

import (
	"context"
	"fmt"
	"testing"
)

func TestNewScan(t *testing.T) {
	c := _newRedisClient()
	s, err := NewScanWithGoRedisV8(
		c,
		KeyConfig{
			Concurrency: 1,
			Amount:      0,
			PageSize:    0,
			Key:         "",
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	err = s.Prepare(context.Background())
	if err != nil {
		t.Fatal("prepare", err)
	}
	s.Scan()

	var finishErr error
	go func() {
		finishErr = s.Finish()
	}()

	amount := 0
	for items := range s.ReceiveChan() {
		l := len(items)
		amount = amount + l
	}

	if finishErr != nil {
		t.Fatal("finish", finishErr)
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
