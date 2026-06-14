package source

import (
	"fmt"
	"testing"
)

func TestNewScan(t *testing.T) {
	c := _newRedisClient()
	s, err := NewScanWithGoRedisV8(KeyConfig{
		Concurrency: 1,
		Amount:      0,
		PageSize:    0,
		KeyName:     "",
	}, c)

	if err != nil {
		t.Fatal(err)
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
