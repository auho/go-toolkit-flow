package destination

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	testredis "github.com/auho/go-toolkit-flow/internal/testutil/redis"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/go-redis/redis/v8"
)

var _redisOptions = testredis.Options

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
	bFunc func(client *redis.Client, config BulkConfig) (*Bulk[E], error),
	buildData func(k *Bulk[E]) int64,
) {
	goredisClient := redis.NewClient(&_redisOptions)

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

	err = k.Finish()
	if err != nil {
		t.Fatal("finish", err)
	}

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
