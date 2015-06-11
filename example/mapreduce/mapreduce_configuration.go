package mapreduce

import "github.com/plutoshe/taskgraph/filesystem"

type MapreduceConfig struct {
	//defined the work num
	WorkNum uint64

	//final result output path
	OutputDir string

	//store the work, appname, and etcdurls

	WorkDir  map[string][]Work
	AppName  string
	EtcdURLs []string

	//optional, define the buffer size
	ReaderBufferSize int
	WriterBufferSize int
}

type Work struct {
	FilesystemClient filesystem.Client
	InputFilePath    string
	userProgram      string
}
