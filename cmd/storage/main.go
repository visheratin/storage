package main

import (
	"github.com/boltdb/bolt"
	"github.com/gorilla/mux"
	"github.com/urfave/negroni"
	"io"
	"log"
	"net/http"
	"storage"
)

var buckets = []string{"catalog", "metadata"}

func initDB(filename string) *bolt.DB {
	db, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		log.Fatal("could not open db")
	}

	db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			tx.CreateBucketIfNotExists([]byte(b))
		}
		return nil
	})
	return db
}

func initRouter(fs *storage.FileService, db *bolt.DB) *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/{path:.*}", NewUploadHandler(fs, db)).Methods("POST")
	r.HandleFunc("/{path:.*}", NewDownloadHandler(fs)).Methods("GET")
	r.HandleFunc("/{path:getcatalog}", NewCatalogHandler(db)).Methods("GET")
	r.HandleFunc("/{path:seekfile}", NewSeekCatalogHandler(db)).Methods("GET")
	r.HandleFunc("/{path:getmetadata}", NewMetadataHandler(db)).Methods("GET")
	return r
}

func NewSeekCatalogHandler(db *bolt.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// err := storage.SeekCatalogFile(db)
	}
}

func NewCatalogHandler(db *bolt.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := storage.GetCatalog(db)
		if err != nil {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	}
}

func NewMetadataHandler(db *bolt.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		storage.GetMetadata(db)

	}
}

func NewDownloadHandler(fs *storage.FileService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		path := vars["path"]
		rd, err := fs.Read(path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			io.Copy(w, rd)
			w.WriteHeader(http.StatusOK)
		}
	}
}

func NewUploadHandler(fs *storage.FileService, db *bolt.DB) http.HandlerFunc {
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
	db := initDB("storage.db")
	fs, _ := storage.NewFileService("test")
	r := initRouter(fs, db)
	n := negroni.Classic()
	n.UseHandler(r)
	log.Fatal(http.ListenAndServe(":8000", n))
}
