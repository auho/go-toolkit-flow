package destination

import (
	"log"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/tests/mysql"
)

var mysqlDsn = mysql.Dsn
var tableName = mysql.DestinationTable
var idName = mysql.IdName
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
	mysql.CleanData(tableName)
}

func tearDown() {
	mysql.CleanData(tableName)
	log.Println("tear down")
}
