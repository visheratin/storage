package storage

import (
	"github.com/boltdb/bolt"
	"github.com/fhs/go-netcdf/netcdf"
	"hash"
	"log"
	"os"
)

func NetcdfFileHandler(f *os.File, db *bolt.DB) {
	df, err := netcdf.OpenFile(f.Name(), netcdf.NOWRITE)
	if err != nil {
		log.Println(err)
	}
	db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte("catalog"))
		c.Put([]byte(hash.Hash(f.Name())), []byte(f.Name()))
		return nil
	})
	ngattrs, err := df.NAttrs()
	if err != nil {
		log.Println(err)
	}
	for i := 0; i < ngattrs; i++ {
		attr, err := df.AttrN(i)
		if err != nil {
			log.Println(err)
		}
		db.Update(func(tx *bolt.Tx) error {
			m := tx.Bucket([]byte("metadata"))
			m.Put([]byte(hash.Hash(attr)), []byte(attr))
			return nil
		})
	}
}
