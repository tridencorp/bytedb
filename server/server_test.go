package server

import (
	"fmt"
	"testing"
)

func TestAdd(t *testing.T) {
	cli := Client{}

	// With prefix
	cmd, arg, err := cli.Add("test::cmd::add::key_1", []byte{})
	
	fmt.Println(cmd, arg, err)
}
