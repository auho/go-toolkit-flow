package destination

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/auho/go-toolkit-flow/storage"
	redis2 "github.com/auho/go-toolkit-flow/tests/redis"
	goredis "github.com/go-redis/redis/v8"
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
	bFunc func(client *goredis.Client, config BulkConfig) (*Bulk[E], error),
	buildData func(k *Bulk[E]) int64,
) {
	goredisClient := goredis.NewClient(&_redisOptions)

	k, err := bFunc(
		goredisClient,
		BulkConfig{
			IsTruncate:  true,
			Concurrency: 1,
			PageSize:    0,
			Key:         key,
		},
	)

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

	dbAmount, err := k.FetchLen()
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
