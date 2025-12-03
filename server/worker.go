package server

import (
	"log"
)

// Worker responsible for file operations
type Worker struct {
	jobs chan []byte
}

// Run worker
func (w *Worker) Run() {
	for {
		select {
		case job := <-w.jobs:
			log.Println("got new job: ", job)
		}
	}
}
