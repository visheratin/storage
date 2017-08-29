package watcher

import (
	"fmt"
	"log"
	"time"

	"github.com/radovskyb/watcher"
)

type WatcherHandler func(string)

type WatcherEvent struct {
	FullPath string
	Op       watcher.Op
}

type Watcher struct {
	Dir    string
	w      *watcher.Watcher
	Events chan WatcherEvent
}

func NewWatcher(dir string) *Watcher {
	w := watcher.New()
	w.IgnoreHiddenFiles(true)

	w.AddRecursive(dir)

	return &Watcher{
		Dir:    dir,
		w:      w,
		Events: make(chan WatcherEvent),
	}
}

func (w *Watcher) Start(rewatch bool) {
	go w.w.Start(1 * time.Second)
	w.Watch(rewatch)
}

func (w *Watcher) Watch(rewatch bool) {
	log.Print("Watching: ", w.Dir)

	if rewatch {
		for k, v := range w.w.WatchedFiles() {
			if !v.IsDir() {
				w.Events <- WatcherEvent{fmt.Sprintf("%s -> %s", k, k), watcher.Rename}
			}
		}
	}

	for {
		select {
		case e := <-w.w.Event:
			if !e.IsDir() {
				w.Events <- WatcherEvent{e.Path, e.Op}
			}
		case err := <-w.w.Error:
			log.Fatal(err)
		case <-w.w.Closed:
			return
		}
	}
}

func (w *Watcher) Stop() {
	w.w.Close()
}
