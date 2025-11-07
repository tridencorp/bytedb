package main

import (
	"bytedb/server"
	"log"
)

func main() {
	// Start database server
	log.Println("Starting database server...")
	server.Run([4]byte{127, 0, 0, 1}, 6666)
}
