package storage

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/bnoon/go-netcdf/netcdf"
	"log"
	"math"
	"path/filepath"
	"strings"
)

const ATTR = "A"
const DIM = "D"
const VAR = "V"
const VARATTR = "VA"

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

func GetAttValue(a netcdf.Attr) (interface{}, error) {
	len, err := a.Len()
	if err != nil {
		return nil, err
	}
	t, err := a.Type()
	if err != nil {
		return nil, err
	}
	switch t {
	case netcdf.INT:
		v := make([]int32, len)
		a.ReadInt32s(v)
		return v[0], nil
	case netcdf.CHAR:
		v := make([]byte, len)
		a.ReadBytes(v)
		return string(v), nil
	case netcdf.FLOAT:
		v := make([]float32, len)
		a.ReadFloat32s(v)
		return v[0], nil
	default:
		return nil, errors.New("Type mismatch")
	}
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
	_, err := db.Exec("INSERT INTO metadata (file_id, type, key, value) VALUES (?,?,?,?)",
		m.FileID,
		m.Type,
		m.Key,
		m.Value.GetValue())
	if err != nil {
		log.Println(err)
	}
}

func NetcdfFileHandler(f string, db *sql.DB) {
	df, err := netcdf.OpenFile(f, netcdf.NOWRITE)
	defer df.Close()
	if err != nil {
		log.Println("here")
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
	any := make(map[string]interface{})
	for i := 0; i < nvars; i++ {
		v := df.VarN(i)
		name, err := v.Name()
		if err != nil {
			log.Println(err, 4)
			continue
		}
		len, err := v.NAttrs()
		if err != nil {
			log.Println(err)
			return
		}
		for j := 0; j < int(len); j++ {
			att, err := v.AttrN(j)
			if err != nil {
				log.Println(err)
				return
			}
			any[att.Name()], err = GetAttValue(att)
			if err != nil {
				log.Println(err)
				return
			}
		}
		md := Metadata{
			FileID: fid,
			Type:   VAR,
			Key:    name,
		}
		var dimstring []string
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
			mdd := Metadata{
				FileID: fid,
				Type:   DIM,
				Key:    n,
			}
			mdd.Value.t = netcdf.INT
			mdd.Value.i = []int32{int32(l)}
			dimstring = append(dimstring, n)
			mds = append(mds, mdd)
		}
		js, err := json.Marshal(any)
		if err != nil {
			log.Println(err)
			return
		}
		mv := Metadata{Type: VARATTR, FileID: fid, Key: name}
		mv.Value.any = js
		mv.Value.t = netcdf.STRING
		md.Value.t = netcdf.STRING
		md.Value.any = strings.Join(dimstring, " ")
		mds = append(mds, md)
		mds = append(mds, mv)
		any = make(map[string]interface{})
	}
	log.Println("Filling catalog for", fid)
	tx, err := db.Begin()
	defer tx.Rollback()
	_, err = db.Exec("DELETE FROM metadata WHERE file_id = ?", fid)
	if err != nil {
		log.Println(err)
		return
	}
	for _, md := range mds {
		md.Save(db)
	}
	tx.Commit()
	log.Println("Catalog filled for", fid)
}

type Coordinate struct {
	Name  string
	Min   float64
	Max   float64
	Index int
}

func Extract(filepath string, varname string, coords []Coordinate) ([]byte, string, error) {
	df, err := netcdf.OpenFile(filepath, netcdf.NOWRITE)
	defer df.Close()
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
					iMin, err := FindValue(c.Min, vv)
					iMax, err := FindValue(c.Max, vv)
					if err != nil {
						return nil, "", err
					}
					offsets[i] = iMin
					lens[i] = iMax - iMin + 1
				} else {
					lens[i] = int(c.Max-c.Min) + 1
					offsets[i] = int(c.Min)
				}
				found = true
				break
			}
		}
		if !found {
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
	case netcdf.DOUBLE:
		floatdata := make([]float64, total)
		err = v.ReadArrayFloat64s(offsets, lens, floatdata)
		if err != nil {
			return nil, "", err
		}
		err = binary.Write(buf, binary.LittleEndian, floatdata)
		if err != nil {
			return nil, "", err
		}
		return buf.Bytes(), "DOUBLE", nil
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
		min := math.MaxInt32
		mindif := math.MaxFloat64
		for i, v2 := range data {
			dif := math.Abs(float64(v2 - val))
			if dif < mindif {
				min = i
			}
			if dif < EPS {
				return i, nil
			}
		}
		return min, nil
	case netcdf.FLOAT:
		data := make([]float32, l)
		err = v.ReadFloat32s(data)
		if err != nil {
			return -1, err
		}
		val := float32(value)
		min := math.MaxInt32
		mindif := math.MaxFloat64
		for i, v2 := range data {
			dif := math.Abs(float64(v2 - val))
			if dif < mindif {
				min = i
			}
			if dif < EPS {
				return i, nil
			}
		}
		return min, nil
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
