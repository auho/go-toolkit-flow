package destination

import (
	"bufio"
	"context"
	"math/rand"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/internal/testutil/file"
)

func TestLine(t *testing.T) {
	d, err := NewLine(Config{
		file.DestinationFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = d.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	d.Accept()

	_rand := rand.Intn(100) + 50
	_max := 0
	go func() {
		var items = []string{"1", "2", "3", "4", "5", "6"}

		for i := 0; i < _rand; i++ {
			err = d.Receive(items)
			if err != nil {
				t.Error(err)
				return
			}

			_max += len(items)
		}

		d.Done()
	}()

	err = d.Finish()
	if err != nil {
		t.Fatal(err)
	}

	err = d.Close()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = os.Remove(file.DestinationFile)
		if err != nil {
			t.Fatal(err)
		}
	}()

	f, err := os.Open(file.DestinationFile)
	if err != nil {
		t.Fatal(err)
	}

	_count := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		_count += 1
	}

	if s.Err() != nil {
		t.Error(s.Err())
	}

	if _count != _max {
		t.Errorf("line is error! expect[%d] != actual[%d]", _max, _count)
	}
}
