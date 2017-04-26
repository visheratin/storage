package netcdf

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bnoon/go-netcdf/netcdf"
	"github.com/hatelikeme/storage/file"
)

const (
	ATTR    = "A"
	DIM     = "D"
	VAR     = "V"
	VARATTR = "VA"
)

type MetadataRequest struct {
	File *file.File
}

type Metadata struct {
	FileID string
	Type   string
	Key    string
	Value  interface{}
}

func NewMetadataRequest(f *file.File) (*MetadataRequest, error) {
	mc := &MetadataRequest{f}
	return mc, nil
}

func attrValue(a netcdf.Attr) (interface{}, error) {
	len, err := a.Len()

	if err != nil {
		return nil, err
	}

	t, err := a.Type()

	if err != nil {
		return nil, err
	}

	switch t {
	case netcdf.SHORT:
		v := make([]int16, len)
		a.ReadInt16s(v)
		return v[0], nil
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
	case netcdf.DOUBLE:
		v := make([]float64, len)
		a.ReadFloat64s(v)
		return v[0], nil
	default:
		return nil, errors.New("Type mismatch")
	}
}

func (mr *MetadataRequest) extractGlobalAttributes(ds netcdf.Dataset) ([]Metadata, error) {
	var mds []Metadata

	ngattrs, err := ds.NAttrs()

	if err != nil {
		return nil, err
	}

	for i := 0; i < ngattrs; i++ {
		a, err := ds.AttrN(i)

		if err != nil {
			return nil, err
		}

		av, err := attrValue(a)

		if err != nil {
			return nil, err
		}

		md := Metadata{
			FileID: mr.File.ID,
			Type:   ATTR,
			Key:    a.Name(),
			Value:  av,
		}

		mds = append(mds, md)
	}

	return mds, nil
}

func extractVariableAttributes(v netcdf.Var) (map[string]interface{}, error) {
	attrs := make(map[string]interface{})

	len, err := v.NAttrs()

	if err != nil {
		return nil, err
	}

	for j := 0; j < int(len); j++ {
		attr, err := v.AttrN(j)

		if err != nil {
			return nil, err
		}

		attrs[attr.Name()], err = attrValue(attr)

		if err != nil {
			return nil, err
		}
	}

	return attrs, nil
}

func (mc *MetadataRequest) extractVariableDimensions(v netcdf.Var) ([]Metadata, error) {
	var mds []Metadata

	dims, err := v.Dims()

	if err != nil {
		return nil, err
	}

	for _, d := range dims {
		n, err := d.Name()

		if err != nil {
			return nil, err
		}

		l, err := d.Len()

		if err != nil {
			return nil, err
		}

		md := Metadata{
			FileID: mc.File.ID,
			Type:   DIM,
			Key:    n,
			Value:  fmt.Sprintf("%v", l),
		}

		mds = append(mds, md)
	}

	return mds, nil
}

func joinKeys(mds []Metadata, sep string) (res string) {
	var keys []string

	for _, m := range mds {
		keys = append(keys, m.Key)
	}

	return strings.Join(keys, sep)
}

func (mc *MetadataRequest) extractVariables(ds netcdf.Dataset) ([]Metadata, error) {
	var mds []Metadata

	nvars, err := ds.NVars()

	if err != nil {
		return nil, err
	}

	for i := 0; i < nvars; i++ {
		v := ds.VarN(i)
		name, err := v.Name()

		if err != nil {
			return nil, err
		}

		dmds, err := mc.extractVariableDimensions(v)

		if err != nil {
			return nil, err
		}

		attrs, err := extractVariableAttributes(v)

		if err != nil {
			return nil, err
		}

		vmd := Metadata{
			FileID: mc.File.ID,
			Type:   VAR,
			Key:    name,
			Value:  joinKeys(dmds, " "),
		}

		ajs, err := json.Marshal(attrs)

		if err != nil {
			return nil, err
		}

		vamd := Metadata{
			FileID: mc.File.ID,
			Type:   VARATTR,
			Key:    name,
			Value:  ajs,
		}

		mds = append(mds, vmd, vamd)
		mds = append(mds, dmds...)
	}

	return mds, nil
}

func (mr *MetadataRequest) Insert(stmt *sql.Stmt) (err error) {
	ds, err := netcdf.OpenFile(mr.File.RealPath, netcdf.NOWRITE)
	defer ds.Close()

	if err != nil {
		return
	}

	var mds []Metadata

	gamds, err := mr.extractGlobalAttributes(ds)

	if err != nil {
		return
	}

	mds = append(mds, gamds...)

	vmds, err := mr.extractVariables(ds)

	if err != nil {
		return
	}

	mds = append(mds, vmds...)

	return insertMetadata(mds, stmt)
}

type MetadataEntry struct {
	Path  string `json:"path"`
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

const allMetadataQuery = "SELECT DISTINCT virt_path, type, key, value FROM files f JOIN metadata m ON f.id = m.file_id"

func DumpMetadata(db *sql.DB) ([]MetadataEntry, error) {
	res, err := db.Query(allMetadataQuery)
	defer res.Close()

	if err != nil {
		return nil, err
	}

	var es []MetadataEntry

	for res.Next() {
		var e MetadataEntry

		err = res.Scan(&e.Path, &e.Type, &e.Key, &e.Value)

		if err != nil {
			return nil, err
		}

		es = append(es, e)
	}

	return es, nil
}

func insertMetadata(mds []Metadata, stmt *sql.Stmt) error {
	for _, md := range mds {
		_, err := stmt.Exec(
			md.FileID,
			md.Type,
			md.Key,
			md.Value)

		if err != nil {
			return err
		}
	}

	return nil
}
