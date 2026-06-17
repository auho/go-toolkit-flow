package source

import (
	"context"
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
	bFunc func(client *redis.Client, config KeyConfig) (*Iterator[E], error),
	c *redis.Client,
	lFunc func(ctx context.Context, c *redis.Client) (int64, error),
) {
	ctx := context.Background()

	k, err := bFunc(
		c,
		KeyConfig{
			Concurrency: 1,
			Amount:      0,
			PageSize:    0,
			Key:         key,
		},
	)

	if err != nil {
		t.Fatal("new", err)
	}

	err = k.Prepare(context.Background())
	if err != nil {
		t.Fatal("prepare", err)
	}
	k.Scan()

	var finishErr error
	go func() {
		finishErr = k.Finish()
	}()

	amount := 0
	for items := range k.ReceiveChan() {
		l := len(items)
		amount = amount + l
	}

	if finishErr != nil {
		t.Fatal("finish", finishErr)
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

func _newRedisClient() *redis.Client {
	return redis.NewClient(&_redisOptions)
}
