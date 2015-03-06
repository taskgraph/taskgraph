package filesystem

type FileSystem interface {
	Open() File
	Exists() bool
	GlobPrefix() []string
}

type File interface {
	Read()
	Write()
	Sync()
	Rename()
}
