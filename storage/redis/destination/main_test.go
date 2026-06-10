package destination

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	redis2 "github.com/auho/go-toolkit-flow/tests/redis"
)

var _redisOptions = redis2.Options

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	log.Println("set up")
	rand.Seed(time.Now().UnixNano())
}

func tearDown() {
	log.Println("tear down")
}

func _testKey[E storage.Entry](
	t *testing.T,
	key string,
	bFunc func(config Config) (*key[E], error),
	buildData func(k *key[E]) int64,
) {
	ctx := context.Background()

	k, err := bFunc(Config{
		IsTruncate:  true,
		Concurrency: 1,
		PageSize:    0,
		Key:         key,
		Options:     &_redisOptions,
	})

	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal("new", err)
	}

	err = k.Accept()
	if err != nil {
		t.Fatal("scan", err)
	}

	amount := int64(0)
	go func() {
		amount = buildData(k)

		k.Done()
	}()

	k.Finish()

	fmt.Println(k.Summary())
	fmt.Println(k.State())

	if k.state.Amount() != amount {
		t.Error(fmt.Sprintf("actual != expected %d != %d", k.state.Amount(), amount))
	}

	dbAmount, err := k.handler.Len(ctx, k.client, k.keyName)
	if err != nil {
		t.Error("db amount ", err)
	}

	if k.state.Amount() != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", k.state.Amount(), dbAmount))
	}

	err = k.Close()
	if err != nil {
		t.Error(err)
	}
}

func _randAmount() int {
	i := int(10e3)
	i += rand.Intn(1000)
	return i
}
