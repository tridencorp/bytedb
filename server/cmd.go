package server

import "bytedb/common"

// All possible command types supported by server
const (
	CmdAdd uint8 = 1
)

// Command arguments
type Args []byte

// Cmd represents server command send by clients
type Cmd struct {
	Type       int8
	Collection uint64
	Namespace  uint64
	Prefix     uint64
	Key        uint64
}

func DecodeCmd(bytes []byte) (*Cmd, Args) {
	cmd := &Cmd{}
	args := Args{}

	common.Decode(
		bytes,
		&cmd.Type,
		&cmd.Collection,
		&cmd.Namespace,
		&cmd.Prefix,
		&cmd.Key,
		(*[]byte)(&args), // cast to bytes so we can avoid reflections
	)

	return cmd, args
}
