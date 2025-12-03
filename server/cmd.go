package server

import "bytedb/common"

// All possible command types supported by server
const (
	CmdAdd uint8 = 1
)

// Cmd represents server command send by clients
type Cmd struct {
	Type       uint8
	Collection uint64
	Namespace  uint64
	Prefix     uint64
	Key        []byte
	Data       []byte
}

func DecodeCmd(buff []byte) *Cmd {
	cmd := &Cmd{}

	common.Decode(
		buff,
		&cmd.Type,
		&cmd.Collection,
		&cmd.Namespace,
		&cmd.Prefix,
		&cmd.Key,
		&cmd.Data,
	)

	return cmd
}
