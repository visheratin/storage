package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/dgraph-io/badger"

	"github.com/gorilla/mux"
	"github.com/hatelikeme/storage"
	"github.com/hatelikeme/storage/netcdf"
	_ "github.com/mattn/go-sqlite3"
	"github.com/urfave/negroni"
)

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

		res := &netcdf.Result{}

		for i := 0; i < 5; i++ {
			res, err = netcdf.Lookup(f, q.Variable, q.Coordinates)

			if err == nil {
				break
			}
		}

		if err == nil {
			json.NewEncoder(w).Encode(res)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

type CatalogEntry struct {
	Path  string              `json:"path"`
	Type  netcdf.MetadataType `json:"type"`
	Key   string              `json:"key"`
	Value string              `json:"value"`
}

func metadataDumpHandler(kv *badger.KV) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		opt := badger.DefaultIteratorOptions

		var catalog []CatalogEntry

		itr := kv.NewIterator(opt)
		for itr.Rewind(); itr.Valid(); itr.Next() {
			item := itr.Item()
			key := item.Key()
			val := item.Value()

			var mds []netcdf.Metadata

			buf := bytes.NewReader(val)
			dec := gob.NewDecoder(buf)

			dec.Decode(&mds)

			for _, md := range mds {
				e := CatalogEntry{
					Path:  string(key),
					Key:   md.Key,
					Type:  md.Type,
					Value: fmt.Sprintf("%s", md.Value),
				}

				catalog = append(catalog, e)
			}

		}
		itr.Close()

		js, err := json.Marshal(catalog)

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

func newRouter(s *storage.Storage, kv *badger.KV) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/download/{path:.*}", downloadHandler(s)).Methods("GET")
	r.HandleFunc("/upload/{path:.*}", uploadHandler(s)).Methods("POST")
	r.HandleFunc("/delete/{path:.*}", deleteHandler(s)).Methods("DELETE")
	r.HandleFunc("/query/{path:.*}", queryHandler(s)).Methods("POST")
	r.HandleFunc("/catalog", metadataDumpHandler(kv)).Methods("GET")
	return r
}

func registerHandlers(s *storage.Storage, kv *badger.KV) {
	s.On(storage.Save, func(e storage.Event) error {
		mds, err := netcdf.ExtractMetadata(e.File)

		if err != nil {
			return err
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		err = enc.Encode(mds)

		if err != nil {
			return err
		}

		kv.Set([]byte(e.File.Path), buf.Bytes(), 0x00)
		return nil
	})
	s.On(storage.Delete, func(e storage.Event) error {
		kv.Delete([]byte(e.File.Path))
		return nil
	})
}

func main() {
	port := flag.Int("port", 8000, "Defaults to 8000")
	dir := flag.String("dbdir", "data", "Defaults to data")
	filedir := flag.String("filedir", "files", "Defaults to files")

	flag.Parse()

	err := os.MkdirAll(*dir, os.ModePerm)

	if err != nil {
		log.Fatal(err)
	}

	opt := badger.DefaultOptions

	opt.Dir = *dir
	opt.ValueDir = *dir

	kv, err := badger.NewKV(&opt)

	if err != nil {
		log.Fatal(err)
	}

	cfg := storage.StorageConfig{
		Dir: *filedir,
	}

	s, err := storage.NewStorage(cfg)

	if err != nil {
		log.Fatal(err)
	}

	registerHandlers(s, kv)

	r := newRouter(s, kv)

	n := negroni.Classic()

	n.UseHandler(r)

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
