package server

import (
	"bytedb/common"
	"fmt"
	"strings"
)

type Client struct {
	conn *Conn
}

// Create new client.
func NewClient(addr string) (*Client, error) {
	conn, err := Connect(addr)
	return &Client{conn: conn}, err
}

// Send ADD command to server.
// Proper key format is "coll::namespace::prefix::key".
func (c *Client) Add(key string, val []byte) ([]byte, error) {
	parts := strings.Split(key, "::")

	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid key")
	}

	cmd := &Cmd{}
	cmd.Collection = Hash([]byte(parts[0]))
	cmd.Namespace  = Hash([]byte(parts[1]))
	cmd.Prefix     = Hash([]byte(parts[2]))
	cmd.Key        = Hash([]byte(parts[3]))

	keyBytes := []byte(parts[3])

	// Original key and val are going to args
	size := len(keyBytes) + len(val)
	args := make(Args, 0, size)

	args = append(args, keyBytes...)
	args = append(args, val...)

	// Prepare cmd pkg
	pkg := common.Encode(
		&cmd.Collection,
		&cmd.Namespace,
		&cmd.Prefix,
		&cmd.Key,
		&args,
	)

	// Send cmd to server
	n, err := c.conn.Write(pkg)
	if err != nil {
		return nil, err
	}

	fmt.Printf("bytes send: %d", n)

	// Wait for response
	res := make([]byte, 4096)
	_, err = c.conn.Read(res)
	if err != nil {
		return nil, err
	}

	fmt.Println(res)
	return args, nil
}
