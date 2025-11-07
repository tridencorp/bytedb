package server

import (
	"sync"
	"syscall"
)

// Wrapper for user connection
type Conn struct {
	fd  int
	mux sync.Mutex
}

// Create new connection
func NewConn(fd int) *Conn {
	return &Conn{fd: fd}
}

// Read from connection
func (c *Conn) Read(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

// Write to connection
func (c *Conn) Write(buf []byte) (int, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return syscall.Write(c.fd, buf)
}

// Close connection
func (c *Conn) Close() error {
	return syscall.Close(c.fd)
}
