package file

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

type File struct {
	VirtualPath string
	RealPath    string
	ID          string
}

type FileService struct {
	Dir string
}

func NewFileService(dir string) (*FileService, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fs := &FileService{dir}
	return fs, nil
}

func (fs *FileService) Resolve(path string) File {
	hasher := md5.New()
	hasher.Write([]byte(path))
	h := hex.EncodeToString(hasher.Sum(nil))
	fp := filepath.Join(fs.Dir, h)
	return File{path, fp, h}
}

func (fs *FileService) Save(f *File, r io.Reader) error {
	fl, err := os.Create(f.RealPath)
	if err != nil {
		return err
	}
	defer fl.Close()
	_, err = io.Copy(fl, r)
	return err
}

func (fs *FileService) Read(f *File, w io.Writer) error {
	fl, err := os.Open(f.RealPath)
	if err != nil {
		return err
	}
	defer fl.Close()
	_, err = io.Copy(w, fl)
	return err
}

func (fs *FileService) Delete(f *File) error {
	return os.Remove(f.RealPath)
}
