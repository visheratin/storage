package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/hatelikeme/storage/netcdf"
	"github.com/hatelikeme/storage/storage"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/negroni"
)

const createMetadataTableQuery = `CREATE TABLE IF NOT EXISTS metadata (
	id      INTEGER PRIMARY KEY,
	path VARCHAR,
	type    VARCHAR,
	key     VARCHAR,
	value   BLOB
)`

func initDB(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc", name))
	db.Exec("PRAGMA journal_mode=WAL")

	if err != nil {
		return nil, err
	}

	_, err = db.Exec(createMetadataTableQuery)

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

		var res *netcdf.Result

		for i := 0; i < 5; i++ {
			res, err = s.Lookup(path, q.Variable, q.Coordinates)
			if err == nil {
				break
			}
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			json.NewEncoder(w).Encode(res)
		}
	}
}

func metadataDumpHandler(s *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mes, err := s.DumpMetadata()

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

func newRouter(s *storage.Storage) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/download/{path:.*}", downloadHandler(s)).Methods("GET")
	r.HandleFunc("/query/{path:.*}", queryHandler(s)).Methods("POST")
	r.HandleFunc("/catalog", metadataDumpHandler(s)).Methods("GET")
	return r
}

func main() {
	dataDir := flag.String("datadir", "./data", "Defaults to ./data")
	dbFile := flag.String("dbfile", "storage.db", "Defaults to storage.db")
	rewatch := flag.Bool("rewatch", false, "Defaults to false")
	port := flag.Int("port", 8000, "Defaults to 8000")
	flag.Parse()

	db, err := initDB(*dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Print("Using sqlite3 database: ", *dbFile)

	err = os.MkdirAll(*dataDir, os.ModePerm)

	if err != nil {
		log.Fatal(err)
	}

	s := storage.New(*dataDir, db)

	s.Start(*rewatch)
	defer s.Stop()

	log.Print("Initialized storage for ", *dataDir)

	r := newRouter(s)

	n := negroni.Classic()

	n.UseHandler(r)

	log.Print("Serving queries at port: ", *port)
	serve(n, fmt.Sprintf(":%v", *port))
}

func serve(h http.Handler, addr string) {
	server := &http.Server{Handler: h}
	l, err := net.Listen("tcp4", addr)

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(server.Serve(l))
}
