package main

import (
	"database/sql"
	"encoding/json"
	"github.com/gorilla/mux"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/negroni"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"github.com/hatelikeme/storage"
	"net"
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
	r.HandleFunc("/query/{path:.*}", NewQueryHandler(fs)).Methods("POST")
	r.HandleFunc("/upload/{path:.*}", NewUploadHandler(fs, db)).Methods("POST")
	r.HandleFunc("/download/{path:.*}", NewDownloadHandler(fs)).Methods("GET")
	r.HandleFunc("/catalog", NewCatalogDumpHandler(db)).Methods("GET")
	r.HandleFunc("/delete/{path:.*}", NewDeleteHandler(fs, db)).Methods("DELETE")
	return r
}

//go:generate msgp
type Query struct {
	Variable    string               `json:"variable"`
	Coordinates []storage.Coordinate `json:"coordinates"`
}

type Result struct {
	Type  string `json:"type"`
	Value []byte `json:"value"`
}

func NewDeleteHandler(fs *storage.FileService, db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		err := fs.DeleteFile(path, db)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}
}

func NewQueryHandler(fs *storage.FileService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		var q Query
		err := json.NewDecoder(r.Body).Decode(&q)
		//err := q.DecodeMsg(msgp.NewReader(r.Body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		vals, tp, err := storage.Extract(filepath.Join(fs.Dir, storage.Resolve(path)), q.Variable, q.Coordinates)
		if err == nil {
			res := Result{tp, vals}
			json.NewEncoder(w).Encode(&res)
			//res.EncodeMsg(msgp.NewWriter(w))
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

type CatalogEntry struct {
	Path  string `json:"path"`
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func NewCatalogDumpHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		res, err := db.Query("SELECT DISTINCT virt_path, type, key, value FROM filetable f JOIN metadata m ON f.id = m.file_id")
		if err != nil {
			log.Println(err)
		}
		defer res.Close()
		var cs []CatalogEntry
		for res.Next() {
			var ce CatalogEntry
			err = res.Scan(&ce.Path, &ce.Type, &ce.Key, &ce.Value)
			if err != nil {
				log.Println(err)
				break
			}
			cs = append(cs, ce)
		}
		js, err := json.Marshal(cs)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		w.Header().Set("content-type", "application/json")
		w.Write(js)
	}
}

func NewDownloadHandler(fs *storage.FileService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		rd, err := fs.Read(path)
		defer rd.Close()
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
		r.Body.Close()
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
	fs, _ := storage.NewFileService("files")
	r := initRouter(fs, db)
	n := negroni.Classic()
	n.UseHandler(r)
	//log.Fatal(http.ListenAndServe(":8000", n))
	server := &http.Server{Handler:  n}
	l, err := net.Listen("tcp4",  ":8000")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Serve(l))
}
