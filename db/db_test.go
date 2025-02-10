package db

import "testing"

func TestOpen(t *testing.T) {
	db, _ := Open("./db")

	if db.root != "./db" {
		t.Errorf("Database root directory was not set")
	}
}