package destination

import (
	"log"
	"os"
	"testing"

	simpledb "github.com/auho/go-simple-db/v3"
	"gorm.io/gorm"

	"github.com/auho/go-toolkit-flow/v3/internal/testutil"
	"github.com/auho/go-toolkit-flow/v3/internal/testutil/mysql"
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
	testutil.LoadEnv()
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	gormDB, simpleDB = mysql.InitDB()

	mysql.CreateTable(gormDB, insertMapTable)
	mysql.CleanData(simpleDB, insertMapTable)
	mysql.CreateTable(gormDB, insertSliceTable)
	mysql.CleanData(simpleDB, insertSliceTable)
	mysql.CreateTable(gormDB, updateMapTable)
	mysql.CleanData(simpleDB, updateMapTable)
}

func tearDown() {
	mysql.CleanData(simpleDB, insertMapTable)
	mysql.CleanData(simpleDB, insertSliceTable)
	mysql.CleanData(simpleDB, updateMapTable)

	err := simpleDB.Close()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("tear down")
}
