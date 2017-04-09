package storage

import (
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
)

type FileService struct {
	Dir string
}

type FileHandler func(file *os.File, db *bolt.DB)

func (fs *FileService) Save(r io.Reader, p string, db *bolt.DB, fh FileHandler) error {
	pt := filepath.Join(fs.Dir, p)
	dir := filepath.Dir(pt)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}

	f, err := os.Create(pt)

	if err != nil {
		log.Println(err)
		return err
	}

	_, err = io.Copy(f, r)
	if err == nil {
		go fh(f, db)
	}
	return err
}

func NewFileService(dir string) (*FileService, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fs := &FileService{dir}
	return fs, nil
}

func (fs *FileService) Read(p string) (io.Reader, error) {
	return os.Open(path.Join(fs.Dir, p))
}

func (fs *FileService) Dispose() error {
	return os.RemoveAll(fs.Dir)
}
