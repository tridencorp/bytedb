package server

import (
	"bytedb/common"
	"fmt"
	"io"
	"net"
	"os"
)

const PrefixLen = 4

// Wrapper for user connection
type Conn struct {
	conn net.Conn
}

// Connect to tcp server,
// Address should be in "ip:port" format
func Connect(address string) (*Conn, error) {
	con, err := net.Dial("tcp", address)
	if err != nil {
		panic(err)
	}

	conn := &Conn{conn: con}
	return conn, nil
}

func FromFD(fd int) *Conn {
	file := os.NewFile(uintptr(fd), "")
	conn, _ := net.FileConn(file)

	return &Conn{conn: conn}
}

// Read from connection, blocking until whole command is read.
func (c *Conn) Read() ([]byte, error) {
	buf := make([]byte, 1024)
	size := uint32(0)

	// 1. Fast path - try to read all at once
	n, err := c.conn.Read(buf)
	if err != nil {
		return nil, err
	}

	if n < PrefixLen {
		return nil, fmt.Errorf("not enough bytes to read pkg size: got %d bytes", n)
	}

	// Decode msg size
	common.Decode(buf[:PrefixLen], &size)

	// Check if we get all data in one read
	if uint32(n-PrefixLen) == size {
		return buf[PrefixLen:n], nil
	}

	// 2. Didn't get all data, we need to read remaining bytes
	total := make([]byte, size)
	offset := copy(total, buf[PrefixLen:n])

	n, err = io.ReadFull(c.conn, total[offset:])
	if err != nil {
		return nil, err
	}

	return total, nil
}

// Write data to connection, blocking until done.
func (c *Conn) Write(buf []byte) (int, error) {
	return c.conn.Write(buf)
}

// Close connection
func (c *Conn) Close() error {
	return c.conn.Close()
}
