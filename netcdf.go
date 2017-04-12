package storage

import (
	"database/sql"
	"github.com/bnoon/go-netcdf/netcdf"
	"log"
	"errors"
	"math"
	"encoding/binary"
)

const ATTR = "A"
const DIM = "D"
const VAR = "V"

type Metadata struct {
	File  *File
	Type  string
	Key   string
	Value ValueHolder
}

type ValueHolder struct {
	c   string
	i   []int32
	f   []float32
	t   netcdf.Type
	any interface{}
}

func (vh *ValueHolder) SetValue(a netcdf.Attr) {
	len, err := a.Len()
	if err != nil {
		log.Println(err, 33)
		return
	}
	vh.t, _ = a.Type()
	switch vh.t {
	case netcdf.CHAR:
		v := make([]byte, len)
		a.ReadBytes(v)
		vh.c = string(v)
	case netcdf.INT:
		v := make([]int32, len)
		a.ReadInt32s(v)
		vh.i = v
	case netcdf.FLOAT:
		v := make([]float32, len)
		a.ReadFloat32s(v)
		vh.f = v
	}
}

func (vh *ValueHolder) GetValue() interface{} {
	switch vh.t {
	case netcdf.CHAR:
		return vh.c
	case netcdf.INT:
		return vh.i[0]
	case netcdf.FLOAT:
		return vh.f[0]
	case netcdf.STRING:
		return vh.any
	default:
		return nil
	}
}

func (m *Metadata) Save(db *sql.DB) {
	res, err := db.Exec("INSERT INTO metadata (file_id, type, key, value) VALUES (?,?,?,?)",
		m.File.Id,
		m.Type,
		m.Key,
		m.Value.GetValue())
	log.Println(res)
	if err != nil {
		log.Println(err)
	}
}

func NetcdfFileHandler(f *File, db *sql.DB) {
	df, err := netcdf.OpenFile(f.FullPath(), netcdf.NOWRITE)
	if err != nil {
		log.Println(err, f.FullPath())
		return
	}
	var mds []Metadata
	ngattrs, err := df.NAttrs()
	if err != nil {
		log.Println(err, 1)
	}
	for i := 0; i < ngattrs; i++ {
		a, err := df.AttrN(i)
		if err != nil {
			log.Println(err, 2)
			return
		}

		md := Metadata{
			File: f,
			Type: ATTR,
			Key:  a.Name(),
		}
		md.Value.SetValue(a)
		mds = append(mds, md)
	}
	nvars, err := df.NVars()
	for i := 0; i < nvars; i++ {
		v := df.VarN(i)
		name, err := v.Name()
		if err != nil {
			log.Println(err, 4)
			continue
		}
		t, err := v.Type()
		if err != nil {
			log.Println(err, 5)
			continue
		}
		md := Metadata{
			File: f,
			Type: VAR,
			Key:  name,
		}
		md.Value.t = netcdf.STRING
		md.Value.any = t
		mds = append(mds, md)
		dims, err := v.Dims()
		for _, d := range dims {
			n, err := d.Name()
			if err != nil {
				log.Println(err, 6)
				continue
			}
			l, err := d.Len()
			if err != nil {
				log.Println(err, 7)
				continue
			}
			md = Metadata{
				File: f,
				Type: DIM,
				Key:  n,
			}
			md.Value.t = netcdf.INT
			md.Value.i = []int32{int32(l)}
			mds = append(mds, md)
		}
	}
	tx, err := db.Begin()
	defer tx.Rollback()
	log.Println(mds)
	for _, md := range mds {
		md.Save(db)
	}
	tx.Commit()
}

type Coordinate struct {
	Name      string
	Value     float64
	Unlimited bool
	Index     int
	Length    int
}

func Extract(filepath string, varname string, coords []Coordinate) ([]byte, error) {
	df, err := netcdf.OpenFile(filepath, netcdf.NOWRITE)
	if err != nil {
		return nil, err
	}
	v, err := df.Var(varname)
	if err != nil{
		return nil, err
	}
	vdims, err := v.Dims()
	if err != nil{
		return nil, err
	}
	if len(coords)==1 {
		uc := coords[0]
		if !uc.Unlimited{
			return nil, errors.New("Single coordinate is not unlimited")
		}
		uvar, err := df.Var(uc.Name)
		if err != nil{
			return nil, err
		}
		nreg, err := FindValue(uc.Value, uvar)
		if err != nil{
			return nil, err
		}
		v.
		if n, _ := udim.Name(); n != uc.Name {
			return nil, errors.New("Unlimited dim names do not match")
		}
		for i := 1; len(vdims) ; i++  {

		}
	}
}

const EPS  = 1e-15

func GetData(v netcdf.Var, offsets []int, lens []int) ([]byte, error) {
	t, err := v.Type()
	if err!= nil{
		return nil, err
	}
	switch t {
	case netcdf.INT:


	}
}

func FindValue(value float64, v netcdf.Var) (int, error) {
	tp, err := v.Type()
	if err != nil{
		return nil, err
	}
	switch tp {
	case netcdf.INT:
		var data []int32
		v.ReadInt32s(data)
		val := int32(value)
		for i, v2 := range data{
			if math.Abs(float64(v2 - val)) < EPS {
				return i, nil
			}
		}
	case netcdf.FLOAT:
		var data []float32
		v.ReadFloat32s(data)
		val := float32(value)
		for i, v2 := range data{
			if math.Abs(float64(v2 - val)) < EPS {
				return i, nil
			}
		}
	default:
		return nil, errors.New("Value not found")
	}
	return nil, errors.New("STRANGE SHIT HAPPENED")
}


/*	for _, c := range coords {
		dv, err := df.Var(c.Name)
		dv.
		if err == nil {
			switch c.Value.(type) {
			case int:
				var tp []int32
				dv.ReadInt32s(tp)
				for j, val := range tp {
					if val == c.Value {
						c.Index = j
						break
					}
				}
			case float32:
				var tp []float32
				dv.ReadFloat32s(tp)
				for j, val := range tp {
					if val == c.Value {
						c.Index = j
						break
					}
				}
			default:
				continue
			}
		} else {
			c.Index = c.Value.(int)
			log.Println(err)
		}
	}
	v, err := df.Var(varname)
	if err != nil {
		return nil, err
	}
	return nil, nil
	*/