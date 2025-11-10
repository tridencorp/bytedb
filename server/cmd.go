package server

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
