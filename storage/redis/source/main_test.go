package source

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
	bFunc func(config Config, client *goredis.Client) (*key[E], error),
	c *goredis.Client,
	lFunc func(ctx context.Context, c *goredis.Client) (int64, error),
) {
	ctx := context.Background()

	k, err := bFunc(Config{
		Concurrency: 1,
		Amount:      0,
		PageSize:    0,
		Key:         key,
	}, c)

	if err != nil {
		t.Fatal("new", err)
	}

	err = k.Scan()
	if err != nil {
		t.Fatal("scan", err)
	}

	amount := 0
	for items := range k.ReceiveChan() {
		l := len(items)
		amount = amount + l
	}

	fmt.Println(k.Summary())
	fmt.Println(k.State())

	if k.total != k.state.Amount() || k.state.Amount() != int64(amount) {
		t.Error(fmt.Sprintf("total != statusAmount != actual %d != %d != %d", k.total, k.state.Amount(), amount))
	}

	dbAmount, err := lFunc(ctx, c)
	if err != nil {
		t.Error("db statusAmount ", err)
	}

	if k.total != dbAmount {
		t.Error(fmt.Sprintf("total != db statusAmount %d != %d", k.total, dbAmount))
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

func _newRedisClient() *goredis.Client {
	return goredis.NewClient(&_redisOptions)
}
