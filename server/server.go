package server

import (
	"log"
	"syscall"
)

// Run TCP server
func Run(address [4]byte, port int) int {
	// Create socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		log.Fatalf("socket: %v", err)
		return -1
	}

	// Enable SO_REUSEADDR
	err = syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if err != nil {
		log.Fatal("setsockopt:", err)
		return -1
	}

	// Set port and address
	s := &syscall.SockaddrInet4{
		Port: port,
		Addr: address,
	}

	// Bind socket
	err = syscall.Bind(fd, s)
	if err != nil {
		log.Fatalf("bind: %v", err)
		return -1
	}

	// Listen on socket
	err = syscall.Listen(fd, syscall.SOMAXCONN)
	if err != nil {
		syscall.Close(fd)
		return -1
	}

	return fd
}
