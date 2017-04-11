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
	"encoding/json"
)

func initSQLite() (db *sql.DB, err error) {
	db, err = sql.Open("sqlite3", "file:storage.db?cache=shared&mode=rwc")
	if err != nil {
		return
	}
	_, err = db.Exec(`create table if not exists filetable (
		virt_path varchar PRIMARY KEY,
		id varchar
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
	r.HandleFunc("/upload/{path:.*}", NewHandler(fs, db)).Methods("POST")
	r.HandleFunc("/upload/{path:.*}", NewUploadHandler(fs, db)).Methods("POST")
	r.HandleFunc("/download/{path:.*}", NewDownloadHandler(fs, db)).Methods("GET")
	r.HandleFunc("/catalog", NewCatalogDumpHandler(db)).Methods("GET")
	return r
}

type CatalogEntry struct {
	Path string
	Type string
	Key string
	Value string
}
func NewCatalogDumpHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := db.Query("SELECT DISTINCT virt_path, type, key, value FROM filetable f JOIN metadata m ON f.id = m.file_id")
		if err != nil{
			log.Println(err)
		}
		defer res.Close()
		var cs []CatalogEntry
		for res.Next(){
			var ce CatalogEntry
			err = res.Scan(&ce.Path, &ce.Type, &ce.Key, &ce.Value)
			if err != nil{
				log.Println(err)
				break
			}
			cs = append(cs, ce)
		}
		js, err := json.Marshal(cs)
		if err!=nil{
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("content-type", "application/json")
		w.Write(js)
	}
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
			log.Println(err)
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
