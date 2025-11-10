package server

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// Wrapper for user connection
type Conn struct {
	fd  int
	mux sync.Mutex
}

// Connect to tcp server,
// Address should be in "ip:port" format
func Connect(address string) (*Conn, error) {
	parts := strings.Split(address, ":")

	// Set addr and port
	ip      := net.ParseIP(parts[0])
	port, _ := strconv.Atoi(parts[1])

	addr := &syscall.SockaddrInet4{
		Port: port,
		Addr: [4]byte(ip.To4()),
	}

	// Create socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}

	// Connect to server
	err = syscall.Connect(fd, addr)
	if err != nil {
		return nil, err
	}

	conn := &Conn{fd: fd}
	return conn, nil
}

// Create new connection
func NewConn(fd int) *Conn {
	return &Conn{fd: fd}
}

// Read from connection. It will block untill all data is read.
func (c *Conn) Read(buf []byte) (int, error) {
	return syscall.Read(c.fd, buf)
}

// Write to connection. It will block untill all data is wrote.
func (c *Conn) Write(buf []byte) (int, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return syscall.Write(c.fd, buf)
}

// Close connection
func (c *Conn) Close() error {
	return syscall.Close(c.fd)
}
