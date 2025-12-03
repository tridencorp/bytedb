package server

import (
	"context"
	"net"
	"syscall"
)

type Server struct {
	Workers []Worker
}

func NewServer() *Server {
	s := &Server{Workers: make([]Worker, 1000)}
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

// Run TCP server.
// Address can be in "0.0.0.0:8080" form.
func Run(address string) (net.Listener, error) {
	// Enable SO_REUSEADDR
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
