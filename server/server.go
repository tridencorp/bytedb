package server

import (
	"log"
	"syscall"
)

// Run TCP server
func Run(address [4]byte, port int) *syscall.SockaddrInet4 {
	// Create socket
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		log.Fatalf("socket: %v", err)
	}

	// Enable SO_REUSEADDR
	if err := syscall.SetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		log.Fatal("setsockopt:", err)
	}

	// Set port and address
	s := &syscall.SockaddrInet4{
		Port: port,
		Addr: address,
	}

	// Bind socket
	if err := syscall.Bind(fd, s); err != nil {
		log.Fatalf("bind: %v", err)
	}

	return s
}
