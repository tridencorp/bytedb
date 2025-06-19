package db

// Available commands
const (
	CmdSet = 0x01
	CmdGet = 0x02
	CmdDel = 0x03
	CmdPut = 0x04
)

type Collection struct {
	// Collection root directory.
	root string
}

func (db *DB) Collection(name string) (*Collection, error) {
	return nil, nil
}
