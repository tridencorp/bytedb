package main

import (
	bit "bytedb/lib/bitbox"
	"bytedb/server"
	"log"
)

func main() {
	log.Println("Starting ByteDB server")

	// create main server
	sock, _ := server.Run("127.0.0.1:6666")

	// run workers
	srv := server.NewServer()
	srv.RunWorkers(1_000)

	// main server loop
	for {
		c, err := sock.Accept()
		if err != nil {
			log.Println("connection error:", err)
			continue
		}

		conn := server.NewConn(c)

		// Each connection is run in separate goroutine.
		// Later we will use poll/epoll together with goroutine pool.
		// I assume that we won't have more than 10k connections at a time.
		go handleConn(srv, conn)
	}
}

func handleConn(srv *server.Server, conn *server.Conn) {
	log.Println("handling connection...")

	// close connection on exit
	defer conn.Close()

	buf := make([]byte, 2048)

	for {
		// read command from user
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Read error or client closed:", err)
			return
		}

		log.Printf("read %d bytes from user", n)
		buf := bit.NewBuffer(buf)

		// decode command length
		cmdLen := uint32(0)
		buf.Decode(&cmdLen)

		// parse command
		cmd := server.DecodeCmd(buf)

		// run command
		_, err = RunCmd(srv, cmd, conn)
		if err != nil {
			log.Println(err)
			// send err resp to user
			continue
		}

		// wait for response
		res := <-conn.Resp
		log.Println(res)
	}
}

// Run command
func RunCmd(srv *server.Server, cmd *server.Cmd, conn *server.Conn) ([]byte, error) {
	switch cmd.Type {
	case server.CmdAdd:
		log.Println("Add new key")

		// send the write request to the file worker
		srv.SendToWorker(cmd)

		return nil, nil
	default:
		log.Printf("unknown command: %d", cmd.Type)
	}

	return nil, nil
}
