package netcdf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"errors"

	"github.com/bnoon/go-netcdf/netcdf"
	"github.com/visheratin/storage/file"
)

func offsetsWithLengths(df netcdf.Dataset, coords []Coordinate, v netcdf.Var) (offsets []int, lens []int, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recovered")
			err = errors.New("Netcdf offsets with lengths paniced")
			offsets = nil
			lens = nil
		}
	}()
	dims, err := v.Dims()

	if err != nil {
		return nil, nil, err
	}

	offsets = make([]int, len(dims))
	lens = make([]int, len(dims))

	for i, dim := range dims {
		l, err := dim.Len()

		if err != nil {
			return nil, nil, err
		}

		lens[i] = int(l)
	}

outer:
	for _, c := range coords {
		for i, dim := range dims {
			n, err := dim.Name()

			if err != nil {
				return nil, nil, err
			}

			if n == c.Name {
				cv, err := df.Var(c.Name)

				if err != nil {
					lens[i] = int(c.Max-c.Min) + 1
					offsets[i] = int(c.Min)
				} else {
					iMin, err := indexOf(c.Min, cv)

					if err != nil {
						return nil, nil, err
					}

					iMax, err := indexOf(c.Max, cv)

					if err != nil {
						return nil, nil, err
					}

					offsets[i] = iMin
					lens[i] = iMax - iMin + 1
				}
				continue outer
			}
		}

		return nil, nil, fmt.Errorf("Coordinate with name %s not found", c.Name)
	}
	return offsets, lens, nil
}

func Lookup(f file.File, varname string, coords []Coordinate) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("Netcdf Lookup paniced: %v", r))
			res = nil
		}
	}()
	df, err := netcdf.OpenFile(f.FullPath, netcdf.NOWRITE)
	if err != nil {
		return nil, err
	}

	v, err := df.Var(varname)
	if err != nil {
		return nil, err
	}

	offsets, lens, err := offsetsWithLengths(df, coords, v)
	if err != nil {
		return nil, err
	}

	res, err = getSlice(v, offsets, lens)
	if err != nil {
		return nil, err
	}

	df.Close()
	return res, nil
}

func getSlice(v netcdf.Var, offsets []int, lens []int) (res *Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recovered")
			err = errors.New("Netcdf getSlice paniced")
			res = nil
		}
	}()
	t, err := v.Type()

	if err != nil {
		return nil, err
	}
	total := 1
	for _, l := range lens {
		total *= l
	}
	buf := new(bytes.Buffer)
	switch t {
	case netcdf.BYTE:
		data := make([]int8, total)
		err = v.ReadArrayInt8s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"BYTE", buf.Bytes()}, nil
	case netcdf.SHORT:
		data := make([]int16, total)
		err = v.ReadArrayInt16s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"SHORT", buf.Bytes()}, nil
	case netcdf.INT:
		data := make([]int32, total)
		err = v.ReadArrayInt32s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"INT", buf.Bytes()}, nil
	case netcdf.INT64:
		data := make([]int64, total)
		err = v.ReadArrayInt64s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"INT64", buf.Bytes()}, nil
	case netcdf.FLOAT:
		data := make([]float32, total)
		err = v.ReadArrayFloat32s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"FLOAT", buf.Bytes()}, nil
	case netcdf.DOUBLE:
		data := make([]float64, total)
		err = v.ReadArrayFloat64s(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		err = binary.Write(buf, binary.LittleEndian, data)
		if err != nil {
			return nil, err
		}
		return &Result{"DOUBLE", buf.Bytes()}, nil
	case netcdf.CHAR:
		data := make([]byte, total)
		err = v.ReadArrayBytes(offsets, lens, data)
		if err != nil {
			return nil, err
		}
		if err != nil {
			return nil, err
		}
		return &Result{"CHAR", data}, nil
	}

	return nil, fmt.Errorf("Type mismatch: %v", t)
}

const eps = 1e-15

func indexOf(value float64, v netcdf.Var) (i int, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Println("recovered")
			err = errors.New("Netcdf indexOf paniced")
			i = 0
		}
	}()
	tp, err := v.Type()
	if err != nil {
		return -1, err
	}

	l, err := v.Len()
	if err != nil {
		return -1, err
	}

	switch tp {
	case netcdf.SHORT:
		data := make([]int16, l)

		err = v.ReadInt16s(data)
		if err != nil {
			return -1, err
		}

		dim := int16(value)

		for i, v2 := range data {
			dif := math.Abs(float64(v2 - dim))
			if dif < eps {
				return i, nil
			}
		}
	case netcdf.INT:
		data := make([]int32, l)

		err = v.ReadInt32s(data)
		if err != nil {
			return -1, err
		}

		dim := int32(value)

		for i, v2 := range data {
			dif := math.Abs(float64(v2 - dim))
			if dif < eps {
				return i, nil
			}
		}
	case netcdf.INT64:
		data := make([]int64, l)

		err = v.ReadInt64s(data)
		if err != nil {
			return -1, err
		}

		dim := int64(value)

		for i, v2 := range data {
			dif := math.Abs(float64(v2 - dim))
			if dif < eps {
				return i, nil
			}
		}
	case netcdf.FLOAT:
		data := make([]float32, l)

		err = v.ReadFloat32s(data)
		if err != nil {
			return -1, err
		}

		dim := float32(value)

		for i, v2 := range data {
			dif := math.Abs(float64(v2 - dim))
			if dif < eps {
				return i, nil
			}
		}
	case netcdf.DOUBLE:
		data := make([]float64, l)

		err = v.ReadFloat64s(data)
		if err != nil {
			return -1, err
		}

		dim := value

		for i, v2 := range data {
			dif := math.Abs(v2 - dim)
			if dif < eps {
				return i, nil
			}
		}
	}
	return -1, fmt.Errorf("Value not found: %v", value)
}
