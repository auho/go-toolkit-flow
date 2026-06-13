package flow

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database/source"
	"github.com/auho/go-toolkit-flow/tests/mysql"
)

var dataSource storage.Source[map[string]any]

func TestMain(m *testing.M) {
	setUp()
	code := m.Run()
	tearDown()
	os.Exit(code)
}

func setUp() {
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
		mysql.DB.GormDB(),
	)
	if err != nil {
		log.Fatal(err)
	}

	mysql.CreateTable(mysql.SourceTable)
	mysql.CreateTable(mysql.DestinationTable)
	mysql.BuildData(mysql.SourceTable)
}

func tearDown() {
	mysql.CleanData(mysql.SourceTable)
	mysql.CleanData(mysql.DestinationTable)
}
