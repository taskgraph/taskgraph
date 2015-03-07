package filesystem

type Client interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Exists(name string) (bool, error)
	Rename(oldpath, newpath string) error
	GlobPrefix(prefix string) ([]string, error)
}

// assume no random access.
type File interface {
	Read(b []byte) (int, error)
	Write(b []byte) (int, error)
	Sync() error
	Close() error
}
