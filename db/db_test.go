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

func TestDelete(t *testing.T) {
	db, _ := Open("./db")
	db.Collection("test")

	db.Delete()

	// Whole database should be removed.
	_, err := os.Stat("./db/")
	if err == nil {
		t.Errorf("Database still exists but should be removed")
	}
}