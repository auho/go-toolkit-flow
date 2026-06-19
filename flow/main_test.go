package flow

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/internal/testutil/mysql"
	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

// setupMySQLTable creates a table and builds test data for MySQL-based tests.
// Each test uses its own table to avoid concurrency contention.
func setupMySQLTable(table string) {
	mysql.CreateTable(table)
	mysql.BuildData(table)
}

// teardownMySQLTable cleans up MySQL test data for the given table.
func teardownMySQLTable(table string) {
	mysql.CleanData(table)
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
