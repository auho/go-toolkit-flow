package mysql

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	simpledb "github.com/auho/go-simple-db/v2"
	mysqlgorm "github.com/auho/go-simple-db/v2/driver/mysql/gorm"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var dbName = "_test_flow"

var SourceTable = "source"
var DestinationTable = "destination"
var IDName = "id"
var NameName = "name"
var ValueName = "value"

func mustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("env var %s not set, please configure MySQL DSN, e.g.: export TEST_MYSQL_DSN='root:pass@tcp(host:port)'", key)
	}
	return v
}

func InitDB() (*gorm.DB, *simpledb.SimpleDB) {
	dsn := mustGetEnv("TEST_MYSQL_DSN") + "/" + dbName

	dbc := &gorm.Config{
		Logger: logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer (log output target, prefix and log content)
			logger.Config{
				SlowThreshold:             time.Second,  // slow SQL threshold
				LogLevel:                  logger.Error, // log level
				IgnoreRecordNotFoundError: true,         // ignore ErrRecordNotFound (record not found) error
			},
		),
	}

	_mysql, err := mysqlgorm.NewMySQL(dsn, dbc)
	if err != nil {
		log.Fatal("mysqlgorm.NewMySQL ", err)
	}

	gormDB := _mysql.GormDB()
	sqlDB, err := gormDB.DB()
	if err != nil {
		log.Fatal("get sql.DB: ", err)
	}
	// Connection pool tuning: the default MaxIdleConns=2 does not match the number of
	// concurrent scan workers (runtime.NumCPU()), causing frequent connection creation/destruction
	// and stale connections triggering retries. Set to 20 to cover typical concurrent scenarios.
	sqlDB.SetMaxOpenConns(20)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	return gormDB, simpledb.NewSimple(_mysql)
}

func CreateTable(db *gorm.DB, table string) {
	err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET `utf8mb4` COLLATE `utf8mb4_general_ci`;", dbName)).Error
	if err != nil {
		log.Fatal("create database ", err)
	}

	query := "CREATE TABLE IF NOT EXISTS `" + dbName + "`.`" + table + "` (" +
		"`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
		"`name` varchar(32) NOT NULL DEFAULT ''," +
		"`value` int(11) NOT NULL DEFAULT '0'," +
		"`created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"PRIMARY KEY (`id`)" +
		") ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;"
	err = db.Exec(query).Error
	if err != nil {
		log.Fatal("create table ", err)
	}
}

func BuildData(db *gorm.DB, table string) {
	err := db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", table)).Error
	if err != nil {
		log.Fatal("build data", err)
	}

	page := int64(rand.Intn(10)) + 10
	pageSize := int64((rand.Intn(4) + 1) * 1000)

	for i := int64(0); i < page; i++ {
		data := make([]map[string]any, pageSize)
		for j := int64(0); j < pageSize; j++ {
			data[j] = map[string]any{
				"name":  fmt.Sprintf("name-%d-%d", i, j),
				"value": i * j,
			}
		}

		err = db.Table(table).Create(data).Error
		if err != nil {
			log.Fatal("bulk insert ", err, data)
		}
	}

	var count int64
	err = db.Table(table).Count(&count).Error
	if err != nil {
		log.Fatal("build data count ", err)
	}

	if count != page*pageSize {
		log.Fatal(fmt.Sprintf("build data bulk insert actual != expected [%d] != [%d]", count, pageSize*page))
	}
}

func CleanData(sdb *simpledb.SimpleDB, table string) {
	err := sdb.Truncate(table)
	if err != nil {
		log.Fatal("clean data", err)
	}
}
