package netcdf

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"github.com/bnoon/go-netcdf/netcdf"
	"github.com/hatelikeme/storage/file"
)

type MetadataType int

const (
	ATTR MetadataType = iota
	DIM
	VAR
	VARATTR
)

type Metadata struct {
	Type  MetadataType
	Key   string
	Value interface{}
}

func attrValue(a netcdf.Attr) (interface{}, error) {
	len, err := a.Len()
	if len == 0 {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	t, err := a.Type()

	if err != nil {
		return nil, err
	}

	switch t {
	case netcdf.BYTE:
		v := make([]int8, len)
		a.ReadInt8s(v)
		return v[0], nil
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
		if math.IsNaN(v[0]) {
			return nil, nil
		}
		return v[0], nil
	default:
		return nil, fmt.Errorf("Type mismatch %s", t)
	}
}

func extractGlobalAttributes(ds netcdf.Dataset) ([]Metadata, error) {
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
			Type:  ATTR,
			Key:   a.Name(),
			Value: av,
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

func tryGetDims(v netcdf.Var) (dims []netcdf.Dim, err error) {
	defer func() {
		recover()
	}()
	return v.Dims()
}

func extractVariableDimensions(v netcdf.Var) (metadata []Metadata, err error) {
	var mds []Metadata

	dims, err := tryGetDims(v)

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
			Type:  DIM,
			Key:   n,
			Value: l,
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

func extractVariables(ds netcdf.Dataset) ([]Metadata, error) {
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

		dmds, err := extractVariableDimensions(v)

		if err != nil {
			return nil, err
		}

		attrs, err := extractVariableAttributes(v)

		if err != nil {
			return nil, err
		}

		vmd := Metadata{
			Type:  VAR,
			Key:   name,
			Value: joinKeys(dmds, " "),
		}

		ajs, err := json.Marshal(attrs)

		if err != nil {
			return nil, err
		}

		vamd := Metadata{
			Type:  VARATTR,
			Key:   name,
			Value: ajs,
		}

		mds = append(mds, vmd, vamd)
		mds = append(mds, dmds...)
	}

	return mds, nil
}

func ExtractMetadata(file *file.File) (mds []Metadata, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Netcdf Lookup paniced: %s", r)
			mds = nil
		}
	}()
	ds, err := netcdf.OpenFile(file.FullPath, netcdf.NOWRITE)

	if err != nil {
		return nil, err
	}

	defer ds.Close()

	gamds, err := extractGlobalAttributes(ds)

	if err != nil {
		return nil, err
	}

	vmds, err := extractVariables(ds)

	if err != nil {
		return nil, err
	}

	mds = append(mds, gamds...)
	mds = append(mds, vmds...)

	return
}
