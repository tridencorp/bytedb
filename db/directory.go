package db

// Manage files and subdirectories.
type Directory struct {
	Dir    string
	PerDir int
	Files  []File
}

func OpenDirectory(dir string, perDir int) *Directory {
	return &Directory{Dir: dir, PerDir: perDir}
}
