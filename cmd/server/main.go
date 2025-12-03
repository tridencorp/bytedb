package main

import (
	bit "bytedb/lib/bitbox"
	"bytedb/server"
	"log"
	"net"
)

func main() {
	log.Println("Starting ByteDB server")

	// Create main server
	sock, _ := server.Run("127.0.0.1:6666")

	// Run workers
	srv := server.NewServer()
	srv.RunWorkers(1_000)

	// Main server loop
	for {
		conn, err := sock.Accept()
		if err != nil {
			log.Println("connection error:", err)
			continue
		}

		// Each connection is run in separate goroutine.
		// Later we will use poll/epoll together with goroutine pool.
		// I assume that we wont have more than 10k connections at a time.
		go handleConn(srv, conn)
	}
}

func handleConn(srv *server.Server, conn net.Conn) {
	log.Println("handling connection...")

	// Close connection on exit
	defer conn.Close()

	buf := make([]byte, 2048)

	for {
		// Read command from user
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Read error or client closed:", err)
			return
		}

		log.Printf("read %d bytes from user", n)
		buf := bit.NewBuffer(buf)

		// Decode command length
		l := uint32(0)
		buf.Decode(&l)

		// Parse command
		cmd := server.DecodeCmd(buf)

		// Run command
		RunCmd(srv, cmd)
	}
}

// Run command
func RunCmd(srv *server.Server, cmd *server.Cmd) []byte {
	switch cmd.Type {
	case server.CmdAdd:
		log.Println("Add new key")

		col := srv.Collection(cmd.Collection)

		log.Println(col)
		return nil
	default:
		log.Printf("unknown command: %d", cmd.Type)
	}

	return nil
}
