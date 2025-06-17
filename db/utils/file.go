package utils

import (
	"os"
)

// Return max entry.
// TODO: refactor it, maybe use Readdirnames.
func MaxEntry(path string, maxFn func(i, j os.DirEntry) bool) os.DirEntry {
	entries, _ := os.ReadDir(path)

	if len(entries) == 0 {
		return nil
	}

	max := entries[0]

	for i := 0; i < len(entries); i++ {
		j := i + 1
		if j >= len(entries) {
			j = i
		}

		if maxFn(entries[i], entries[j]) {
			max = entries[j]
		}
	}

	return max
}
