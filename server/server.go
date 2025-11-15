package server

import (
	"log"
	"syscall"
)

// Main server class
type Server struct {
}

// Get collection from cache
func (s *Server) Collection(hash uint64) {
}

// Run TCP server
func Run(address [4]byte, port int) int {
	// Create socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		log.Panic(err)
	}

	// Enable SO_REUSEADDR
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)

	// Set port and address
	s := &syscall.SockaddrInet4{
		Port: port,
		Addr: address,
	}

	// Bind socket
	err = syscall.Bind(fd, s)
	if err != nil {
		log.Panic(err)
	}

	// Listen on socket
	err = syscall.Listen(fd, syscall.SOMAXCONN)
	if err != nil {
		syscall.Close(fd)
		log.Panic(err)
	}

	return fd
}
