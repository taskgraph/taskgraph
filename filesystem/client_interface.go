package filesystem

import "io"

type Client interface {
	// Opens the file for reading.
	OpenReadCloser(name string) (io.ReadCloser, error)
	// Opens the file for writing.
	// It creates the file if it didn't exist.
	OpenWriteCloser(name string) (io.WriteCloser, error)
	Exists(name string) (bool, error)
	Rename(oldpath, newpath string) error
	// Glob returns the names of all files matching pattern or
	// nil if there is no matching file.
	Glob(pattern string) (matches []string, err error)
	// Remove specific file in filesystem
	Remove(name string) error
}
