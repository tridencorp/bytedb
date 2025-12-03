package server

import (
	"fmt"
	"testing"
)

func TestAdd(t *testing.T) {
	cli, err := NewClient("127.0.0.1:6666")
	if err != nil {
		fmt.Println(err)
		return
	}

	res, err := cli.Add("test::cmd::prefix::key_1", []byte("Hello"))
	fmt.Println(res, err)
}
