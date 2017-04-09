package storage

import (
	"fmt"
	"github.com/boltdb/bolt"
)

func GetCatalog(db *bolt.DB) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("catalog"))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Printf("key=%s, value=%s\n", k, v)
		}
		return nil
	})
}

func SeekCatalogFile(db *bolt.DB, filename string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("catalog"))
		c := b.Cursor()
		k, v := c.Seek([]byte(filename))
		if k != nil {
			fmt.Printf("value exists key=%s, value=%s\n", k, v)
			return nil
		} else {
			return error("request for value failed")
		}
		return nil
	})
}

func GetMetadata(db *bolt.DB) {

}
