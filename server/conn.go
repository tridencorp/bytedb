package server

import (
	bit "bytedb/lib/bitbox"
	"fmt"
	"io"
	"net"
	"os"
)

const PrefixLen = 4

// User connection. Wrapper for net.Conn.
type Conn struct {
	conn net.Conn
	Resp chan *[]byte
}

func NewConn(conn net.Conn) *Conn {
	c := &Conn{conn: conn, Resp: make(chan *[]byte)}
	return c
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

// Create Conn from file descriptor
func FromFD(fd int) *Conn {
	file := os.NewFile(uintptr(fd), "")
	conn, _ := net.FileConn(file)

	return &Conn{conn: conn}
}

// Read from connection, blocking until all data is read.
func (c *Conn) Read(buf []byte) (int, error) {
	size := uint32(0)

	// fast path - try to read all at once
	n, err := c.conn.Read(buf)
	if err != nil {
		return 0, err
	}

	if n < PrefixLen {
		return 0, fmt.Errorf("not enough bytes to read pkg size: got %d bytes need %d", n, PrefixLen)
	}

	// decode msg size
	buff := bit.NewBuffer(buf[:PrefixLen])
	bit.Decode(buff, &size)

	// check if we read all data
	if uint32(n-PrefixLen) == size {
		return n, nil // success
	}

	// we didn't get all data, try to read remaining bytes
	total := make([]byte, size)
	offset := copy(total, buf[PrefixLen:n])

	n, err = io.ReadFull(c.conn, total[offset:])
	if err != nil {
		return 0, err
	}

	return n, nil // success
}

// Write data to connection, blocking until done.
func (c *Conn) Write(buf []byte) (int, error) {
	return c.conn.Write(buf)
}

// Close connection
func (c *Conn) Close() error {
	return c.conn.Close()
}
