package storage

import (
	"database/sql"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/hatelikeme/storage/file"
	"github.com/radovskyb/watcher"

	"github.com/hatelikeme/storage/netcdf"
)

type Storage struct {
	fs      *file.FileService
	db      *sql.DB
	stopped chan bool
}

func New(dir string, db *sql.DB) *Storage {
	d, err := filepath.Abs(dir)

	if err != nil {
		log.Fatal(err)
	}

	return &Storage{
		fs:      file.NewFileService(d),
		db:      db,
		stopped: make(chan bool),
	}
}

func (s *Storage) Start(rewatch bool) {
	s.fs.Start(rewatch)
	go s.listen()
}

func (s *Storage) Stop() {
	s.fs.Stop()
	s.stopped <- true
}

func (s *Storage) listen() {
	for {
		select {
		case e := <-s.fs.Watcher.Events:
			log.Printf("%s: %s\n", e.Op, e.FullPath)

			switch e.Op {
			case watcher.Create:
				f, err := s.fs.FromFullPath(e.FullPath)

				if err != nil {
					log.Println(err)
				}

				go s.insertMetadata(f)
			case watcher.Remove:
				f, err := s.fs.FromFullPath(e.FullPath)

				if err != nil {
					log.Println(err)
				}

				go s.cleanMetadata(f)
			case watcher.Rename:
				tks := strings.Split(e.FullPath, " -> ")
				rm, _ := s.fs.FromFullPath(tks[0])
				cr, _ := s.fs.FromFullPath(tks[1])
				go func() {
					s.cleanMetadata(rm)
					s.insertMetadata(cr)
				}()
			}
		case <-s.stopped:
			return
		}
	}
}

const insertMetadataQuery = "INSERT INTO metadata (path, type, key, value) VALUES (?,?,?,?)"
const cleanMetadataQuery = "DELETE FROM metadata WHERE path = ?"
const allMetadataQuery = "SELECT DISTINCT path, type, key, value FROM metadata"

func (s *Storage) insertMetadata(f *file.File) error {
	mds, err := netcdf.ExtractMetadata(f)

	if err != nil {
		return err
	}

	for _, md := range mds {
		_, err := s.db.Exec(insertMetadataQuery, md.Path, md.Type, md.Key, md.Value)

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) cleanMetadata(f *file.File) error {
	_, err := s.db.Exec(cleanMetadataQuery, f.RelPath)

	return err
}

func (s *Storage) Read(path string, w io.Writer) error {
	f := s.fs.FromRelPath(path)

	return s.fs.Read(f, w)
}

func (s *Storage) Lookup(path string, varname string, coords []netcdf.Coordinate) (*netcdf.Result, error) {
	f := s.fs.FromRelPath(path)

	return netcdf.Lookup(f, varname, coords)
}

func (s *Storage) DumpMetadata() ([]netcdf.MetadataEntry, error) {
	res, err := s.db.Query(allMetadataQuery)
	defer res.Close()

	if err != nil {
		return nil, err
	}

	var es []netcdf.MetadataEntry

	for res.Next() {
		var e netcdf.MetadataEntry

		err = res.Scan(&e.Path, &e.Type, &e.Key, &e.Value)

		if err != nil {
			return nil, err
		}

		es = append(es, e)
	}

	return es, nil
}
