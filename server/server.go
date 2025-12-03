package server

import (
	"context"
	"net"
	"syscall"
)

type Server struct {
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
