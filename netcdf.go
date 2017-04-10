package storage

import (
	"database/sql"
	"github.com/fhs/go-netcdf/netcdf"
	"log"
	"os"
)

func NetcdfFileHandler(f *os.File, db *sql.DB) {
	df, err := netcdf.OpenFile(f.Name(), netcdf.NOWRITE)
	if err != nil {
		log.Println(err)
	}
	if err != nil {
		log.Println(err)
	}

	ngattrs, err := df.NAttrs()
	if err != nil {
		log.Println(err)
	}
	for i := 0; i < ngattrs; i++ {
		_, err := df.AttrN(i)
		if err != nil {
			log.Println(err)
		}
		//TODO
	}
}