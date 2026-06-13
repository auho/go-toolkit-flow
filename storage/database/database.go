package database

import (
	simpledb "github.com/auho/go-simple-db/v2"
)

type Driver interface {
	DB() *DB
}

type GenDB func() (*DB, error)

type DB struct {
	*simpledb.SimpleDB
}

func NewDB(sdb *simpledb.SimpleDB) *DB {
	return &DB{SimpleDB: sdb}
}

func BuildDB(fn func() (*simpledb.SimpleDB, error)) (*DB, error) {
	sdb, err := fn()
	if err != nil {
		return nil, err
	}

	return NewDB(sdb), nil
}
