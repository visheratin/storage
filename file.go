package storage

import (
	"database/sql"
	"io"
	"os"
	"path"
	"path/filepath"
	"encoding/hex"
	"crypto/md5"
)

type FileService struct {
	Dir string
}

type FileHandler func(path string, db *sql.DB)

func (fs *FileService) Save(r io.Reader, p string, db *sql.DB, fh FileHandler) (err error) {
	id := Resolve(p)
	fpath := filepath.Join(fs.Dir, id)
	fl, err := os.Create(fpath)
	if err != nil {
		return
	}
	_, err = io.Copy(fl, r)
	if err != nil {
		return
	}
	_, err = db.Exec("INSERT OR REPLACE INTO filetable (virt_path, id) VALUES (?, ?)", p, id)
	if err != nil{
		return
	}
	go fh(fpath, db)
	return nil
}

func Resolve(path string) string {
	hasher := md5.New()
	hasher.Write([]byte(path))
	return hex.EncodeToString(hasher.Sum(nil))
}

func (fs *FileService)DeleteFile(path string, db *sql.DB) error {
	id := Resolve(path)
	_, err := db.Exec("DELETE FROM filetable WHERE virt_path = ?", path)
	_, err = db.Exec("DELETE FROM metadata WHERE file_id = ?", id)
	err = os.Remove(filepath.Join(fs.Dir, Resolve(path)))
	if err != nil{
		return err
	}
	return nil
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
	id := Resolve(p)
	return os.Open(path.Join(fs.Dir, id))
}

func (fs *FileService) Dispose() error {
	return os.RemoveAll(fs.Dir)
}
