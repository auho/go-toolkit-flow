package destination

import (
	"log"
	"os"
	"testing"

	simpledb "github.com/auho/go-simple-db/v2"
	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
	"gorm.io/gorm"
)

var insertMapTable = "destination_insert_map"
var insertSliceTable = "destination_insert_slice"
var updateMapTable = "destination_update_map"
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

	mysql.CreateTable(insertMapTable)
	mysql.CleanData(insertMapTable)
	mysql.CreateTable(insertSliceTable)
	mysql.CleanData(insertSliceTable)
	mysql.CreateTable(updateMapTable)
	mysql.CleanData(updateMapTable)
}

func tearDown() {
	mysql.CleanData(insertMapTable)
	mysql.CleanData(insertSliceTable)
	mysql.CleanData(updateMapTable)

	err := simpleDB.Close()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("tear down")
}
