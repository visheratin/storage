package storage

import (
	"database/sql"
	"github.com/nu7hatch/gouuid"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
)

type FileService struct {
	Dir string
}

type FileHandler func(file *os.File, db *sql.DB)

func (fs *FileService) Save(r io.Reader, p string, db *sql.DB, fh FileHandler) error {
	pt := filepath.Join(fs.Dir, p)
	dir := filepath.Dir(pt)
	res, err := db.Query("SELECT real_path FROM filetable WHERE real_path = " + dir)
	if err != nil {
		log.Println(err)
	}
	defer res.Close()
	res.Next()
	var rpath string = ""
	err = res.Scan(&rpath)
	if err != nil {
		log.Println(err)
	}
	if rpath == "" {
		newid, err := uuid.NewV4()
		if err != nil {
			log.Println(err)
		}
		_, err = db.Exec("INSERT INTO filetable (virt_path, real_path) VALUES (" + newid.String() + ", " + pt + ")")
		if err != nil {
			log.Println(err)
		}
		f, err := os.Create(fs.Dir)	//TODO

		if err != nil {
			log.Println(err)
			return err
		}

		_, err = io.Copy(f, r)
		if err == nil {
			go fh(f, db)
		}
	} else {
		res, err = db.Query("SELECT virt_path FROM filetable WHERE real_path = " + rpath)
		if err != nil {
			log.Println(err)
		}
		defer res.Close()
		res.Next()
		var olduuid string
		res.Scan(&olduuid)
		db.Exec("INSERT INTO filetable (virt_path, real_path) VALUES (" + olduuid + ", " + dir + ")")

		f, err := os.Create(dir)

		if err != nil {
			log.Println(err)
			return err
		}

		_, err = io.Copy(f, r)
		if err == nil {
			go fh(f, db)
		}
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

func (fs *FileService) Read(p string, db *sql.DB) (io.Reader, error) {
	res, err := db.Query("SELECT virt_path FROM filetable WHERE real_path =" + path.Join(fs.Dir, p))
	if err!=nil{
		log.Println(err)
	}
	defer res.Close()
	res.Next()
	var vpath string
	res.Scan(&vpath)

	os.Open(path.Join(fs.Dir, p))
	return nil, nil //TODO
}

func (fs *FileService) Dispose() error {
	return os.RemoveAll(fs.Dir)
}
