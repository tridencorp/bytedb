package db

import (
	"fmt"
	"testing"
)

func TestOpenHash(t *testing.T) {
	db, _ := Open("./db")
	defer db.Delete()

	col, _ := db.Collection("test")
	_, err := OpenHash(col)
	if err != nil {
		fmt.Printf("err: %s\n", err)
		panic(err)
	}
}
