package db

import (
	"os"
	"testing"
)

func TestOpen(t *testing.T) {
	db, _ := Open("./db")

	if db.root != "./db" {
		t.Errorf("Database root directory was not set")
	}
}

func TestCollection(t *testing.T) {
	db, _ := Open("./db")
	db.Collection("test")

	// At this point, we only want to check if the proper
	// directories have been created.
	_, err := os.Stat("./db/collections/test/1.bucket")
	if err != nil {
		t.Errorf("Collection path doesn't exist")
	}
}
