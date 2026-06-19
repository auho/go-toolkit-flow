package destination

import (
	"log"
	"os"
	"testing"

	simpledb "github.com/auho/go-simple-db/v2"
	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
	"gorm.io/gorm"
)

var tableName = mysql.DestinationTable
var idName = mysql.IDName
var nameName = mysql.NameName
var valueName = mysql.ValueName
var gormDB *gorm.DB
var simpleDB *simpledb.SimpleDB

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	gormDB, simpleDB = mysql.InitDB()

	mysql.CreateTable(tableName)
	mysql.CleanData(tableName)
}

func tearDown() {
	mysql.CleanData(tableName)

	err := simpleDB.Close()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("tear down")
}
