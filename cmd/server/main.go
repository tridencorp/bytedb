package main

import (
	"bytedb/server"
	"log"
	"syscall"
)

func main() {
	log.Println("Starting database server...")

	// Create database server
	fd := server.Run([4]byte{127, 0, 0, 1}, 6666)

	// Close listening socket
	defer syscall.Close(fd)

	// Accept connections
	for {
		nfd, addr, err := syscall.Accept(fd)
		if err != nil {
			log.Println("accept error:", err)
			continue
		}

		// For this version each connection will be run in separate goroutine.
		// Later we will use poll/epoll together with goroutine pool.
		go handleConn(nfd, addr)
	}
}

func handleConn(nfd int, addr syscall.Sockaddr) {
	log.Println("handling connection...")
}
