package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/julienschmidt/httprouter"
	_ "github.com/mattn/go-sqlite3"
	"github.com/visheratin/storage/netcdf"
	"github.com/visheratin/storage/storage"
)

var s *storage.Storage
var db *sql.DB

const createMetadataTable = `CREATE TABLE IF NOT EXISTS metadata (
	id INTEGER PRIMARY KEY,
	path    VARCHAR,
	type    VARCHAR,
	key     VARCHAR,
	value   BLOB
)`

const insertMetadata = "INSERT INTO metadata (path, type, key, value) VALUES (?,?,?,?)"
const cleanMetadata = "DELETE FROM metadata WHERE path = ?"

func createDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc", name))

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

func queryHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	path := ps.ByName("path")

	path = strings.Replace(path, "```", "/", -1)
	var q Query
	err := json.NewDecoder(r.Body).Decode(&q)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	f := s.Resolve(path)
	res := &netcdf.Result{}
	res, err = netcdf.Lookup(f, q.Variable, q.Coordinates)

	if err == nil {
		b, err := res.MarshalMsg(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		_, err = w.Write(b)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func metadataDumpHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
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

func downloadHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	path := ps.ByName("path")
	s.Read(path, w)
}

func uploadHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	path := ps.ByName("path")
	err := s.Save(path, r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
	} else {
		w.WriteHeader(http.StatusAccepted)
	}
}

func deleteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	path := ps.ByName("path")
	err := s.Delete(path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func newRouter(s *storage.Storage, db *sql.DB) *httprouter.Router {
	r := httprouter.New()
	r.GET("/download/:path", downloadHandler)
	r.POST("/upload/:path", uploadHandler)
	r.DELETE("/delete/:path", deleteHandler)
	r.POST("/query/:path", queryHandler)
	r.GET("/catalog", metadataDumpHandler)
	return r
}

func registerHandlers(s *storage.Storage, db *sql.DB) {
	cmq, _ := db.Prepare(cleanMetadata)
	imq, _ := db.Prepare(insertMetadata)

	s.On(storage.Save, func(e storage.Event) error {
		_, err := cmq.Exec(e.File.Path)
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
		_, err := cmq.Exec(e.File.Path)
		return err
	})
}

func main() {
	port := flag.String("port", "8000", "Defaults to 8000")

	flag.Parse()
	var err error
	db, err = createDB("storage.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	cfg := storage.StorageConfig{
		Dir: "files",
	}
	s, err = storage.NewStorage(cfg)
	if err != nil {
		log.Fatal(err)
	}

	registerHandlers(s, db)

	r := newRouter(s, db)

	http.ListenAndServe(":"+*port, r)
}
