package server

import (
	"bytedb/common"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

const PrefixLen = 4

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
	ip := net.ParseIP(parts[0])
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, err
	}

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

// Read from connection, blocking until whole command is read
func (c *Conn) Read() ([]byte, error) {
	buf := make([]byte, 1024)
	size := uint32(0)

	// 1. Fast path - try to read all at once
	n, err := syscall.Read(c.fd, buf)
	if err != nil {
		return nil, err
	}

	if n < PrefixLen {
		return nil, fmt.Errorf("short read: got %d bytes", n)
	}

	// Decode msg size
	common.Decode(buf[:PrefixLen], &size)

	// Check if we get all data in one read
	if uint32(n-PrefixLen) == size {
		return buf[PrefixLen:n], nil
	}

	// 2. Didn't get all data, we need to read remaining bytes
	total := make([]byte, 0, size)
	total = append(total, buf[PrefixLen:n]...)

	for uint32(len(total)) < size {
		n, err = syscall.Read(c.fd, buf)
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, io.ErrUnexpectedEOF // connection closed
		}

		total = append(total, buf[:n]...)
	}

	return total, nil
}

// Write data to connection, blocking until done
func (c *Conn) Write(buf []byte) (int, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return syscall.Write(c.fd, buf)
}

// Close connection
func (c *Conn) Close() error {
	return syscall.Close(c.fd)
}
