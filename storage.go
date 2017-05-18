package storage

import (
	"io"
	"log"

	"github.com/hatelikeme/storage/file"
)

type EventType string

const (
	Save   EventType = "SAVE"
	Delete EventType = "DELETE"
	Read   EventType = "READ"
)

type Event struct {
	File *file.File
	Type EventType
}

type Storage struct {
	Config      StorageConfig
	fileService *file.FileService
	handlers    map[EventType][]EventHandler
}

type StorageConfig struct {
	Dir string
}

func NewStorage(cfg StorageConfig) (*Storage, error) {
	fs, err := file.NewFileService(cfg.Dir)
	if err != nil {
		return nil, err
	}
	return &Storage{
		Config:      cfg,
		fileService: fs,
		handlers:    make(map[EventType][]EventHandler),
	}, nil
}

func (s *Storage) On(evt EventType, h EventHandler) {
	hs, ok := s.handlers[evt]

	if !ok {
		hs = make([]EventHandler, 0)
	}

	hs = append(hs, h)
	s.handlers[evt] = hs
	log.Printf("Added handler %v for event %s", h, evt)
}

type EventHandler func(Event) error

func (s *Storage) Resolve(path string) file.File {
	return s.fileService.Resolve(path)
}

func (s *Storage) trigger(e Event) (err error) {
	log.Printf("Triggering handlers for event: %v", e)
	hs := s.handlers[e.Type]
	for _, h := range hs {
		err = h(e)
		if err != nil {
			return err
		}
	}
	return
}

func (s *Storage) apply(path string, fn func(*file.File) error, evt EventType) error {
	f := s.Resolve(path)
	fp := &f
	err := fn(fp)
	if err != nil {
		return nil
	}
	e := Event{fp, evt}
	return s.trigger(e)
}

func (s *Storage) Save(path string, r io.Reader) error {
	return s.apply(path, func(f *file.File) error {
		return s.fileService.Save(f, r)
	}, Save)
}

func (s *Storage) Delete(path string) error {
	return s.apply(path, func(f *file.File) error {
		return s.fileService.Delete(f)
	}, Delete)
}

func (s *Storage) Read(path string, w io.Writer) error {
	return s.apply(path, func(f *file.File) error {
		return s.fileService.Read(f, w)
	}, Read)
}
