package main

import (
	"database/sql"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/negroni"
	"io"
	"log"
	"net/http"
	"storage"
)

func initSQLite() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", "storage.db")
	if err != nil {
		return
	}
	_, err = db.Exec(`create table if not exists filetable (
		virt_path varchar PRIMARY KEY,
		uuid varchar
	)`)
	if err != nil {
		return
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS metadata (
		id      INTEGER PRIMARY KEY,
		file_id VARCHAR,
		type    char(1),
		key     VARCHAR,
		value   BLOB,
		FOREIGN KEY(file_id) REFERENCES filetable (id)
	)`)
	return
}

func initRouter(fs *storage.FileService, db *sql.DB) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/{path:.*}", NewUploadHandler(fs, db)).Methods("POST")
	r.HandleFunc("/{path:.*}", NewDownloadHandler(fs, db)).Methods("GET")
	//r.HandleFunc("/{path:/catalog}", NewCatalogQueryHandler(fs)).Methods("POST")
	return r
}

func NewDownloadHandler(fs *storage.FileService, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		rd, err := fs.Read(path, db)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			io.Copy(w, rd)
			w.WriteHeader(http.StatusOK)
		}
	}
}

func NewUploadHandler(fs *storage.FileService, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		err := fs.Save(r.Body, path, db, storage.NetcdfFileHandler)
		if err != nil {
			w.WriteHeader(http.StatusConflict)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}
}

func main() {
	db, err := initSQLite()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fs, _ := storage.NewFileService("test")
	r := initRouter(fs, db)
	n := negroni.Classic()
	n.UseHandler(r)
	log.Fatal(http.ListenAndServe(":8000", n))
}
