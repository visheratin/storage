package file

import (
	"io"
	"os"
	"path/filepath"
)

type File struct {
	Path     string
	FullPath string
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
	fp := filepath.Join(fs.Dir, path)
	return File{path, fp}
}

func (fs *FileService) Save(f *File, r io.Reader) error {
	err := os.MkdirAll(filepath.Dir(f.FullPath), os.ModePerm)
	if err != nil {
		return err
	}
	fl, err := os.Create(f.FullPath)
	if err != nil {
		return err
	}
	defer fl.Close()
	_, err = io.Copy(fl, r)
	return err
}

func (fs *FileService) Read(f *File, w io.Writer) error {
	fl, err := os.Open(f.FullPath)
	if err != nil {
		return err
	}
	defer fl.Close()
	_, err = io.Copy(w, fl)
	return err
}

func (fs *FileService) Delete(f *File) error {
	return os.Remove(f.FullPath)
}
