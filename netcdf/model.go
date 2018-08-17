package netcdf

//go:generate msgp

type Result struct {
	Type  string
	Value []byte
}

type Coordinate struct {
	Name  string
	Min   float64
	Max   float64
	Index int
}
