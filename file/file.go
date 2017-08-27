package file

import (
	"io"
	"os"
	"path/filepath"

	"github.com/hatelikeme/storage/watcher"
)

type FileService struct {
	Dir     string
	Watcher *watcher.Watcher
}

type File struct {
	RelPath  string
	FullPath string
}

func NewFileService(dir string) *FileService {
	w := watcher.NewWatcher(dir)

	fs := &FileService{dir, w}

	return fs
}

func (fs *FileService) Start(rewatch bool) {
	go fs.Watcher.Start(rewatch)
}

func (fs *FileService) Stop() {
	fs.Watcher.Stop()
}

func (fs *FileService) FromRelPath(path string) *File {
	fp := filepath.Join(fs.Dir, path)
	return &File{path, fp}
}

func (fs *FileService) FromFullPath(path string) (*File, error) {
	rp, err := filepath.Rel(fs.Dir, path)

	if err != nil {
		return nil, err
	}

	return &File{rp, path}, nil
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
