package storage

import (
	"database/sql"
	"hash/fnv"
	"io"
	"os"
	"path"
	"path/filepath"
	"errors"
)

type FileService struct {
	Dir string
}

type File struct {
	Id       string
	VirtPath string
	Dir      string
	file     *os.File
}

func (f *File) Save(db *sql.DB) (err error) {
	dbid := fnv.New64()
	dbid.Write([]byte(f.VirtPath))
	f.Id = string(dbid.Sum(nil))
	_, err = db.Exec("INSERT INTO filetable (virt_path, id) VALUES (?, ?)", f.VirtPath, f.Id)
	return
}

func (f *File) Write(b []byte) (int, error) {
	return f.file.Write(b)
}

func (f *File) FullPath() string {
	return filepath.Join(f.Dir, f.Id)
}

func NewFile(vp string, dir string) (f *File, err error) {
	if vp == "" {
		return nil, errors.New("File path is empty")
	}

	hash32 := fnv.New32a()
	hash32.Write([]byte(vp))
	f.Id = string(hash32.Sum32())
	f.VirtPath = vp
	f.Dir = dir
	f.file, err = os.Create(f.FullPath())
	return f, nil
}

type FileHandler func(file *File, db *sql.DB)

func (fs *FileService) Save(r io.Reader, p string, db *sql.DB, fh FileHandler) (err error) {
	fl, err := NewFile(p, fs.Dir)
	if err != nil {
		return
	}
	err = fl.Save(db)
	if err != nil {
		return
	}

	if err != nil {
		return err
	}

	_, err = io.Copy(fl, r)
	if err == nil {
		go fh(fl, db)
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

func (fs *FileService) Read(p string, db *sql.DB) (io.Reader, error) {
	res, err := db.Query("SELECT id FROM filetable WHERE virt_path = ?", p)
	defer res.Close()
	if err != nil {
		return nil, err
	}
	res.Next()
	var id string
	res.Scan(&id)

	return os.Open(path.Join(fs.Dir, id))
}

func (fs *FileService) Dispose() error {
	return os.RemoveAll(fs.Dir)
}
