package filesystem

import "io"

type Client interface {
	OpenReadCloser(name string) (io.ReadCloser, error)
	OpenWriteCloser(name string) (io.WriteCloser, error)
	Exists(name string) (bool, error)
	Rename(oldpath, newpath string) error
	Glob(dirname, pattern string) ([]string, error)
}
