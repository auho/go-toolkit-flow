package database

import (
	goSimpleDb "github.com/auho/go-simple-db/v2"
)

type BuildDb func() (*DB, error)

type DB struct {
	*goSimpleDb.SimpleDB
}

func NewDB(fn func() (*goSimpleDb.SimpleDB, error)) (*DB, error) {
	sd, err := fn()
	if err != nil {
		return nil, err
	}

	return NewFromSimpleDb(sd), nil
}

func NewFromSimpleDb(sd *goSimpleDb.SimpleDB) *DB {
	return &DB{SimpleDB: sd}
}

type Driver interface {
	DB() *DB
}
