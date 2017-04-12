package storage

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"github.com/bnoon/go-netcdf/netcdf"
	"log"
	"math"
	"path/filepath"
)

const ATTR = "A"
const DIM = "D"
const VAR = "V"

type Metadata struct {
	FileID string
	Type   string
	Key    string
	Value  ValueHolder
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
		m.FileID,
		m.Type,
		m.Key,
		m.Value.GetValue())
	log.Println(res)
	if err != nil {
		log.Println(err)
	}
}

func NetcdfFileHandler(f string, db *sql.DB) {
	df, err := netcdf.OpenFile(f, netcdf.NOWRITE)
	if err != nil {
		log.Println(err, f)
		return
	}
	fid := filepath.Base(f)
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
			FileID: fid,
			Type:   ATTR,
			Key:    a.Name(),
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
			FileID: fid,
			Type:   VAR,
			Key:    name,
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
				FileID: fid,
				Type:   DIM,
				Key:    n,
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
	Name  string
	Value float64
	Index int
}

func Extract(filepath string, varname string, coords []Coordinate) ([]byte, string, error) {
	df, err := netcdf.OpenFile(filepath, netcdf.NOWRITE)
	if err != nil {
		return nil, "", err
	}
	v, err := df.Var(varname)
	if err != nil {
		return nil, "", err
	}
	vdims, err := v.Dims()
	if err != nil {
		return nil, "", err
	}

	offsets := make([]int, len(vdims))
	lens := make([]int, len(vdims))
	for i, val := range vdims {
		n, err := val.Name()
		if err != nil {
			return nil, "", err
		}
		l, err := val.Len()
		found := false
		for _, c := range coords {
			if n == c.Name {
				vv, err := df.Var(c.Name)
				if err == nil {
					nreg, err := FindValue(c.Value, vv)
					if err != nil {
						return nil, "", err
					}
					offsets[i] = nreg
				} else {
					offsets[i] = int(c.Value)
				}
				found = true
				break
			}
		}
		if found {
			lens[i] = 1
		} else {
			lens[i] = int(l)
		}
	}
	log.Println(offsets, lens)
	data, tp, err := GetData(v, offsets, lens)
	if err != nil {
		return nil, "", err
	}
	return data, tp, nil
}

const EPS = 1e-15

func GetData(v netcdf.Var, offsets []int, lens []int) ([]byte, string, error) {
	t, err := v.Type()
	log.Println(t)
	if err != nil {
		return nil, "", err
	}
	buf := new(bytes.Buffer)
	total := 1
	for _, l := range lens {
		total *= l
	}
	switch t {
	case netcdf.SHORT:
		intdata := make([]int16, total)
		err = v.ReadArrayInt16s(offsets, lens, intdata)
		if err != nil {
			return nil, "", err
		}
		log.Println(intdata)
		err = binary.Write(buf, binary.LittleEndian, intdata)
		if err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "SHORT", nil
	case netcdf.INT:
		intdata := make([]int32, total)
		err = v.ReadArrayInt32s(offsets, lens, intdata)
		if err != nil {
			return nil, "", err
		}
		err = binary.Write(buf, binary.LittleEndian, intdata)
		if err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "INT", nil
	case netcdf.FLOAT:
		floatdata := make([]float32, total)
		err = v.ReadArrayFloat32s(offsets, lens, floatdata)
		if err != nil {
			return nil, "", err
		}
		err = binary.Write(buf, binary.LittleEndian, floatdata)
		if err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "FLOAT", nil
	default:
		return nil, "", errors.New("Type mismatch")
	}
}

func FindValue(value float64, v netcdf.Var) (int, error) {
	tp, err := v.Type()
	if err != nil {
		return -1, err
	}
	l, err := v.Len()
	switch tp {
	case netcdf.INT:
		data := make([]int32, l)
		err = v.ReadInt32s(data)
		if err != nil {
			return -1, err
		}
		val := int32(value)
		for i, v2 := range data {
			if math.Abs(float64(v2-val)) < EPS {
				return i, nil
			}
		}
	case netcdf.FLOAT:
		data := make([]float32, l)
		v.ReadFloat32s(data)
		val := float32(value)
		for i, v2 := range data {
			if math.Abs(float64(v2-val)) < EPS {
				return i, nil
			}
		}
	}
	return -1, errors.New("Value not found")
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
