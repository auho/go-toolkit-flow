package source

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/auho/go-toolkit-flow/internal/testutil/file"
)

func TestNewLine(t *testing.T) {
	_max := rand.Intn(50) + 50
	_buildFile(t, _max)

	s, err := NewLine(Config{
		Name:      file.SourceFile,
		BatchSize: rand.Intn(50) + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = s.Prepare(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.Scan()

	var finishErr error
	go func() {
		finishErr = s.Finish()
	}()

	for items := range s.ReceiveChan() {
		_ = items
	}

	if finishErr != nil {
		t.Fatal(finishErr)
	}

	if s.state.Amount() != int64(_max) {
		t.Fatalf("expect[%d] != actual[%d]", _max, s.state.Amount())
	}

	defer func() {
		err = os.Remove(file.SourceFile)
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = s.Close()
	if err != nil {
		t.Error(err)
	}
}

func _buildFile(t *testing.T, max int) {
	f, err := os.Create(file.SourceFile)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < max; i++ {
		_, err = f.WriteString(strconv.Itoa(i) + "\n")
		if err != nil {
			t.Fatal(err)
		}
	}

	err = f.Close()
	if err != nil {
		t.Fatal(err)
	}
}
