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

		// Create Conn
		conn := server.NewConn(nfd)

		// For this version each connection will be run in separate goroutine.
		// Later we will use poll/epoll together with goroutine pool.
		go handleConn(conn, addr)
	}
}

func handleConn(conn *server.Conn, addr syscall.Sockaddr) {
	log.Println("handling connection...")

	// We want to be sure that connection will be always closed
	defer conn.Close()

	// Buffer for reading data
	buf := make([]byte, 4096)

	for {
		// Waiting for data to read
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Read error or client closed:", err)
			return
		}

		if n == 0 {
			log.Println("Client closed connection")
			return
		}

		msg := string(buf[:n])
		log.Printf("Received from %v: %s", addr, msg)
	}
}
