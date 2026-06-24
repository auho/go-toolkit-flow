package flow

import (
	"log"
	"math/rand"
	"os"
	"testing"

	simpledb "github.com/auho/go-simple-db/v3"
	"gorm.io/gorm"

	"github.com/auho/go-toolkit-flow/v3/internal/testutil"
	"github.com/auho/go-toolkit-flow/v3/internal/testutil/mysql"
	"github.com/auho/go-toolkit-flow/v3/storage"
	"github.com/auho/go-toolkit-flow/v3/storage/database/source"
)

var _gormDB *gorm.DB
var _simpleDB *simpledb.SimpleDB

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	_gormDB, _simpleDB = mysql.InitDB()
	code := m.Run()
	os.Exit(code)
}

// setupMySQLTable creates a table and builds test data for MySQL-based tests.
// Each test uses its own table to avoid concurrency contention.
func setupMySQLTable(table string) {
	mysql.CreateTable(_gormDB, table)
	mysql.BuildData(_gormDB, table)
}

// teardownMySQLTable cleans up MySQL test data for the given table.
func teardownMySQLTable(table string) {
	mysql.CleanData(_simpleDB, table)
}

// buildDataSource creates a database source reading from the given table.
func buildDataSource(table string) storage.Source[map[string]any] {
	gormDB, _ := mysql.InitDB()
	dataSrc, err := source.NewSectionMapWithGorm(
		source.SectionConfig{
			Concurrency: 0,
			PageSize:    rand.Int63n(177) + 93,
		},
		source.ScanConfig{
			TableName:     table,
			SegmentIDName: mysql.IDName,
			SelectFields:  []string{mysql.NameName, mysql.ValueName},
		},
		gormDB,
	)
	if err != nil {
		log.Fatal(err)
	}

	return dataSrc
}
