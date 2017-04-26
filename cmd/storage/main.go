package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hatelikeme/storage"
	"github.com/hatelikeme/storage/netcdf"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/negroni"
)

const createFilesTable = `CREATE TABLE IF NOT EXISTS files (
	virt_path VARCHAR PRIMARY KEY,
	id VARCHAR
)`

const createMetadataTable = `CREATE TABLE IF NOT EXISTS metadata (
	id      INTEGER PRIMARY KEY,
	file_id VARCHAR,
	type    VARCHAR,
	key     VARCHAR,
	value   BLOB,
	FOREIGN KEY(file_id) REFERENCES filetable (id)
)`

const upsertFile = "INSERT OR REPLACE INTO files (virt_path, id) VALUES (?, ?)"
const insertMetadata = "INSERT INTO metadata (file_id, type, key, value) VALUES (?,?,?,?)"
const cleanMetadata = "DELETE FROM metadata WHERE file_id = ?"
const deleteFile = "DELETE FROM files WHERE virt_path = ?"

func createDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc", name))

	if err != nil {
		return nil, err
	}
	_, err = db.Exec(createFilesTable)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(createMetadataTable)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type Query struct {
	Variable    string              `json:"variable"`
	Coordinates []netcdf.Coordinate `json:"coordinates"`
}

func queryHandler(s *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]

		var q Query
		err := json.NewDecoder(r.Body).Decode(&q)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		f := s.Resolve(path)
		res, err := netcdf.Lookup(f, q.Variable, q.Coordinates)

		if err == nil {
			json.NewEncoder(w).Encode(res)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func metadataDumpHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mes, err := netcdf.DumpMetadata(db)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		js, err := json.Marshal(mes)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("content-type", "application/json")
		w.Write(js)
	}
}

func downloadHandler(s *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		s.Read(path, w)
	}
}

func uploadHandler(s *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		err := s.Save(path, r.Body)
		defer r.Body.Close()
		if err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
		} else {
			w.WriteHeader(http.StatusAccepted)
		}
	}
}

func deleteHandler(s *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		err := s.Delete(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}
}

func newRouter(s *storage.Storage, db *sql.DB) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/download/{path:.*}", downloadHandler(s)).Methods("GET")
	r.HandleFunc("/upload/{path:.*}", uploadHandler(s)).Methods("POST")
	r.HandleFunc("/delete/{path:.*}", deleteHandler(s)).Methods("DELETE")
	r.HandleFunc("/query/{path:.*}", queryHandler(s)).Methods("POST")
	r.HandleFunc("/catalog", metadataDumpHandler(db)).Methods("GET")
	return r
}

func registerHandlers(s *storage.Storage, db *sql.DB) {
	ufq, _ := db.Prepare(upsertFile)
	cmq, _ := db.Prepare(cleanMetadata)
	imq, _ := db.Prepare(insertMetadata)
	dfq, _ := db.Prepare(deleteFile)

	s.On(storage.Save, func(e storage.Event) error {
		_, err := cmq.Exec(e.File.ID)
		return err
	})
	s.On(storage.Save, func(e storage.Event) error {
		_, err := ufq.Exec(e.File.VirtualPath, e.File.ID)
		return err
	})
	s.On(storage.Save, func(e storage.Event) error {
		mr, err := netcdf.NewMetadataRequest(e.File)

		if err != nil {
			return err
		}

		tx, err := db.Begin()
		defer tx.Rollback()

		if err != nil {
			return err
		}

		tximq := tx.Stmt(imq)
		defer tximq.Close()

		err = mr.Insert(tximq)

		if err != nil {
			return err
		}

		return tx.Commit()
	})

	s.On(storage.Delete, func(e storage.Event) error {
		_, err := cmq.Exec(e.File.ID)
		return err
	})
	s.On(storage.Delete, func(e storage.Event) error {
		_, err := dfq.Exec(e.File.VirtualPath)
		return err
	})
}

func main() {
	db, err := createDB("storage.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	cfg := storage.StorageConfig{
		Dir: "files",
	}
	s, err := storage.NewStorage(cfg)
	if err != nil {
		log.Fatal(err)
	}

	registerHandlers(s, db)

	r := newRouter(s, db)

	n := negroni.Classic()

	n.UseHandler(r)

	serve(n, ":8000")
}

func serve(h http.Handler, addr string) {
	server := &http.Server{Handler: h}
	l, err := net.Listen("tcp4", addr)

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(server.Serve(l))
}
