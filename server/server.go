package server

import (
	"bytedb/db"
	"context"
	"log"
	"net"
	"syscall"
)

type Server struct {
	Workers     []Worker                  // file workers
	Collections map[uint64]*db.Collection // opened collections
}

func NewServer() *Server {
	s := &Server{
		Workers:     make([]Worker, 1_000),
		Collections: make(map[uint64]*db.Collection),
	}

	return s
}

// Run workers, each one in separate goroutine
func (s *Server) RunWorkers(n int) {
	for i := 0; i < n; i++ {
		w := Worker{}
		s.Workers = append(s.Workers, w)

		go w.Run()
	}
}

// Send job to worker
func (s *Server) SendToWorker(cmd *Cmd) error {
	coll, err := s.Collection(cmd.Collection)
	if err != nil {
		return err
	}

	log.Println(coll)
	return nil
}

// Return collection for the given hash
func (s *Server) Collection(hash uint64) (*db.Collection, error) {
	// get collection from memory
	col, ok := s.Collections[hash]
	if !ok {
		return col, nil
	}

	// if that failed, read it from disk
	return nil, nil
}

// Run TCP server.
// Address can be in "0.0.0.0:8080" form.
func Run(address string) (net.Listener, error) {
	// config for enabling SO_REUSEADDR
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			c.Control(func(fd uintptr) {
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})

			return nil
		},
	}

	return lc.Listen(context.Background(), "tcp", address)
}
