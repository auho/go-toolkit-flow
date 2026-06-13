package source

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/tests/mysql"
)

var mysqlDsn = mysql.Dsn
var tableName = mysql.SourceTable
var idName = mysql.IDName
var nameName = mysql.NameName
var valueName = mysql.ValueName

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	mysql.CreateTable(tableName)
	mysql.BuildData(tableName)
}

func _testSection[E storage.Entry](
	t *testing.T,
	s *Section[E],
) {
	err := s.Scan()
	if err != nil {
		t.Error("scan ", err)
	}

	amount := 0
	for items := range s.ReceiveChan() {
		l := len(items)
		amount = amount + l
	}

	fmt.Println(s.Summary())
	fmt.Println(s.State())

	if s.total != s.state.Amount() && s.state.Amount() != int64(amount) {
		t.Error(fmt.Sprintf("total != amount != actual %d != %d != %d", s.total, s.state.Amount(), amount))
	}
	var dbAmount int64
	db := s.DB()
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
	mysql.CleanData(tableName)
	log.Println("tear down")
}
