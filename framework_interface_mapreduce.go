package taskgraph

import "./filesystem"

// This interface is used by mapreduce application during taskgraph configuration phase.
type MapreduceBootstrap interface {
	Bootstrap
	InitWithMapreduceConfig(MapreduceConfig)
}

type MapreduceConfig struct {
	MapperNum  uint64
	ShuffleNum uint64
	ReducerNum uint64

	//filesystem
	FilesystemClient filesystem.Client
	OutputDir        string
	InterDir         string

	//emit function
	MapperFunc  func(MapreduceTask, string)
	ReducerFunc func(MapreduceTask, string, []string)

	//work
	UserDefined bool
	WorkDir     map[string][]Work
	AppName     string
	EtcdURLs    []string

	//optional
	ReaderBufferSize int
	WriterBufferSize int
}

type Work struct {
	Config map[string]string
}
