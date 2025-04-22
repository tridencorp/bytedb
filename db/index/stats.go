package index

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime/pprof"
)

// Run profiler and display results to STDOUT.
// For now, only CPU profile is available.
type Profiler struct {
	tmp *os.File
}

// Start cpu profile.
func (p *Profiler) Start() {
	tmp, err := os.CreateTemp("", "cpu.pprof")
	if err != nil {
		fmt.Println("Could not create temp file:", err)
		return
	}

	p.tmp = tmp

	if err := pprof.StartCPUProfile(p.tmp); err != nil {
		fmt.Println("Could not start CPU profile:", err)
		return
	}
}

func (p *Profiler) Stop() {
	pprof.StopCPUProfile()
}

// Run pprof with 'top' command.
func (p *Profiler) Info() {
	defer os.Remove(p.tmp.Name())

	cmd := exec.Command("go", "tool", "pprof", p.tmp.Name())

	cmd.Stdin = bytes.NewBufferString("top\nquit\n")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		fmt.Println("Error running pprof:", err)
	}
}

// Basic stats for indexes and key collisions.
type Stats struct {
}
