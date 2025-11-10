package server

import (
	"bytedb/common"
	"fmt"
	"strings"
)

type Client struct {
}

// Create new client.
func NewClient(addr string) (*Client, error) {
	// conn, err := Connect(addr)
	return nil, nil
}

// Send ADD command to server.
// Proper key format is "coll::namespace::prefix::key".
func (c *Client) Add(key string, val []byte) (*Cmd, []byte, error) {
	parts := strings.Split(key, "::")

	if len(parts) < 4 {
		return nil, nil, fmt.Errorf("invalid key")
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

	fmt.Println(pkg)
	// Send cmd to server

	Connect("127.0.0.1:4000")

	return cmd, args, nil
}
