package main

import (
	"bytes"
	"database/sql"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/dgraph-io/badger"
	"github.com/hatelikeme/storage/netcdf"
	_ "github.com/mattn/go-sqlite3"
)

const createMetadataTable = `CREATE TABLE IF NOT EXISTS metadata (
	id INTEGER PRIMARY KEY,
	path    VARCHAR,
	type    VARCHAR,
	key     VARCHAR,
	value   BLOB
)`

const selectAll = "SELECT path, type, key, value FROM metadata ORDER BY path, type, key"

func createDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc", name))

	if err != nil {
		return nil, err
	}
	_, err = db.Exec(createMetadataTable)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type Metadata struct {
	Path  string
	Type  netcdf.MetadataType
	Key   string
	Value interface{}
}

func main() {
	dbfile := flag.String("sqlitefile", "storage.db", "Defaults to storage.db")
	dbdir := flag.String("kvdir", "db", "Defaults to data")

	flag.Parse()

	db, err := createDB(*dbfile)

	if err != nil {
		log.Fatal(err)
	}

	err = os.MkdirAll(*dbdir, os.ModePerm)

	if err != nil {
		log.Fatal(err)
	}

	opt := badger.DefaultOptions

	opt.Dir = *dbdir
	opt.ValueDir = *dbdir

	kv, err := badger.NewKV(&opt)

	if err != nil {
		log.Fatal(err)
	}

	defer kv.Close()

	res, err := db.Query(selectAll)

	if err != nil {
		log.Fatal(err)
	}

	var total int

	row := db.QueryRow("SELECT COUNT(*) FROM metadata")

	row.Scan(&total)

	var mds []Metadata

	var previous string
	var current string
	var i int

	for res.Next() {
		i = i + 1
		md := Metadata{}

		var typ string
		var val interface{}

		err := res.Scan(&md.Path, &typ, &md.Key, &val)

		switch typ {
		case "A":
			md.Type = netcdf.ATTR
			md.Value = fmt.Sprintf("%s", val)
		case "D":
			md.Type = netcdf.DIM
			md.Value, err = strconv.ParseInt(string(val.([]byte)), 10, 64)
			if err != nil {
				log.Fatal(err)
			}
		case "V":
			md.Type = netcdf.VAR
			md.Value = fmt.Sprintf("%s", val)
		case "VA":
			md.Type = netcdf.VARATTR
			md.Value = val.([]byte)
		}

		if err != nil {
			log.Fatal(err)
		}

		current = md.Path
		if previous == "" {
			previous = current
		}

		if current != previous || i == total {
			log.Printf("Migrating metadata for path: %s", previous)
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)

			err = enc.Encode(mds)

			if err != nil {
				log.Fatal(err)
			}

			err = kv.Set([]byte(previous), buf.Bytes(), 0x00)

			if err != nil {
				log.Fatal(err)
			}

			mds = mds[:0]
		}

		mds = append(mds, md)

		previous = current
	}

	defer db.Close()

}
