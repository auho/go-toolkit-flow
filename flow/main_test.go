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

var dataSource storage.Source[map[string]any]

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
	mysql.CreateTable(mysql.SourceTable)
	mysql.CreateTable(mysql.DestinationTable)
	mysql.BuildData(mysql.SourceTable)
}

func tearDown() {
	mysql.CleanData(mysql.SourceTable)
	mysql.CleanData(mysql.DestinationTable)
}

func buildDataSource() {
	gormDB, _ := mysql.InitDB()

	var err error
	dataSource, err = source.NewSectionMapWithGorm(
		source.SectionConfig{
			Concurrency: 0,
			PageSize:    rand.Int63n(177) + 93,
		},
		source.ScanConfig{
			TableName:     mysql.SourceTable,
			SegmentIDName: mysql.IDName,
			SelectFields:  []string{mysql.NameName, mysql.ValueName},
		},
		gormDB,
	)
	if err != nil {
		log.Fatal(err)
	}
}
