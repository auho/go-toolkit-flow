package source

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	simpledb "github.com/auho/go-simple-db/v2"
	"gorm.io/gorm"

	"github.com/auho/go-toolkit-flow/internal/testutil"
	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
	"github.com/auho/go-toolkit-flow/storage"
)

var tableName = mysql.SourceTable
var idName = mysql.IDName
var nameName = mysql.NameName
var valueName = mysql.ValueName
var gormDB *gorm.DB
var simpleDB *simpledb.SimpleDB

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	gormDB, simpleDB = mysql.InitDB()

	mysql.CreateTable(gormDB, tableName)
	mysql.BuildData(gormDB, tableName)
}

func _testSection[E storage.Entry](
	t *testing.T,
	s *Section[E],
	db *gorm.DB,
) {
	err := s.Prepare(context.Background())
	if err != nil {
		t.Error("prepare ", err)
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
		t.Error("finish ", finishErr)
	}

	fmt.Println(s.Summary())
	fmt.Println(s.StateString())

	if s.total != s.state.Amount() && s.state.Amount() != int64(amount) {
		t.Error(fmt.Sprintf("total != amount != actual %d != %d != %d", s.total, s.state.Amount(), amount))
	}
	var dbAmount int64
	if db == nil {
		t.Error("db is nil")
		return
	}
	err = db.Table(tableName).Count(&dbAmount).Error
	if err != nil {
		t.Error("db amount ", err)
	}

	if s.total != dbAmount {
		t.Error(fmt.Sprintf("total != db amount %d != %d", s.total, dbAmount))
	}
}

func tearDown() {
	mysql.CleanData(simpleDB, tableName)
	log.Println("tear down")
}
