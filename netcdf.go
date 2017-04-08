package storage

import (
	"github.com/fhs/go-netcdf/netcdf"
	"os"
)

func NetcdfFileHandler(f *os.File){
	netcdf.OpenFile(f.Name(), netcdf.NOWRITE)
}