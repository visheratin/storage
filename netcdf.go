package storage

import (
	"database/sql"
	"github.com/bnoon/go-netcdf/netcdf"
	"log"
	"strconv"
)

const ATTR = 'A'
const DIM = 'D'
const VAR = 'V'

type Metadata struct {
	File  *File
	Type  rune
	Key   string
	Value []byte
}

func (m *Metadata) Save(db *sql.DB) {
	db.Exec("INSERT INTO metadata (file_id, type, key, value) VALUES (?,?,?,?)",
		m.File.Id,
		m.Type,
		m.Key,
		m.Value)
}

func NetcdfFileHandler(f *File, db *sql.DB) {
	df, err := netcdf.OpenFile(f.FullPath(), netcdf.NOWRITE)
	if err != nil {
		log.Println(err)
		return
	}
	mds := make([]Metadata, 128)
	ngattrs, err := df.NAttrs()
	if err != nil {
		log.Println(err)
	}
	for i := 0; i < ngattrs; i++ {
		a, err := df.AttrN(i)
		if err != nil {
			log.Println(err)
			return
		}
		v, err := a.GetBytes()
		if err != nil {
			log.Println(err)
			return
		}
		md := Metadata{
			f,
			ATTR,
			a.Name(),
			v,
		}
		mds = append(mds, md)
	}
	nvars, err := df.NVars()
	for i := 0; i < nvars; i++ {
		v := df.VarN(i)
		name, err := v.Name()
		if err != nil {
			log.Println(err)
			continue
		}
		t, err := v.Type()
		if err != nil {
			log.Println(err)
			continue
		}
		md := Metadata{
			f,
			VAR,
			name,
			[]byte(t.String()),
		}
		mds = append(mds, md)
		dims, err := v.Dims()
		for _, d := range dims {
			n, err := d.Name()
			if err != nil {
				log.Println(err)
				continue
			}
			l, err := d.Len()
			if err != nil {
				log.Println(err)
				continue
			}
			md = Metadata{
				f,
				DIM,
				n,
				[]byte(strconv.Itoa(int(l))),
			}
			mds = append(mds, md)
		}
	}
	for _, md := range mds {
		md.Save(db)
	}
}
