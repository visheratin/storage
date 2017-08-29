package workerpool

import "errors"
import "fmt"

type Job func() error

type WorkerPool struct {
	Errors chan error
	jobs   chan Job
	closed chan bool
}

func New(n int) *WorkerPool {
	wp := &WorkerPool{
		Errors: make(chan error, 256),
		jobs:   make(chan Job, 1024),
		closed: make(chan bool),
	}

	for i := 0; i < n; i++ {
		w := &Worker{wp}
		go w.Loop()
	}

	return wp
}

func (wp *WorkerPool) Submit(j Job) {
	wp.jobs <- j
}

func (wp *WorkerPool) Close() {
	wp.closed <- true
}

type Worker struct {
	wp *WorkerPool
}

func (w *Worker) Loop() {
	for {
		select {
		case j := <-w.wp.jobs:
			w.Execute(j)
		case <-w.wp.closed:
			return
		}
	}
}

func (w *Worker) Execute(j Job) {
	defer func() {
		if r := recover(); r != nil {
			w.wp.Errors <- errors.New(fmt.Sprintf("Worker recovered after panic: %s", r))
		}
	}()

	err := j()

	if err != nil {
		w.wp.Errors <- err
	}
}
