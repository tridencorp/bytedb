package server

import (
	bit "bytedb/lib/bitbox"
	"fmt"
	"log"
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
// Key format is "coll::namespace::prefix::key".
func (c *Client) Add(key string, val []byte) ([]byte, error) {
	parts := strings.Split(key, "::")

	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid key")
	}

	cmd := &Cmd{}
	cmd.Type = uint8(CmdAdd)
	cmd.Collection = Hash([]byte(parts[0]))
	cmd.Namespace = Hash([]byte(parts[1]))
	cmd.Prefix = Hash([]byte(parts[2]))
	cmd.Key = []byte(parts[3])
	cmd.Data = val

	// prepare cmd req
	req := bit.Encode(
		&cmd.Type,
		&cmd.Collection,
		&cmd.Namespace,
		&cmd.Prefix,
		&cmd.Key,
		&cmd.Data,
	)

	// add length prefix
	req = bit.Encode(req)

	// send cmd to server
	n, err := c.conn.Write(req)
	if err != nil {
		return nil, err
	}

	log.Println("sending: ", cmd.Collection, cmd.Namespace, cmd.Prefix, cmd.Key)
	log.Printf("bytes send: %d", n)

	// read response
	res := make([]byte, 1024)
	_, err = c.conn.Read(res)
	if err != nil {
		return nil, err
	}

	fmt.Println(res)
	return res, nil
}
