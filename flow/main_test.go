package flow

import (
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/auho/go-toolkit-flow/storage"
	"github.com/auho/go-toolkit-flow/storage/database"
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
	dataSource, err = source.NewSectionSliceMap(&source.QueryConfig{
		Config: source.Config{
			Concurrency: 0,
			PageSize:    rand.Int63n(177) + 93,
			TableName:   mysql.SourceTable,
			IdName:      mysql.IdName,
		},
		Fields: []string{mysql.NameName, mysql.ValueName},
		Where:  "",
		Order:  "",
	}, func() (*database.DB, error) {
		return mysql.DB, nil
	})
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
