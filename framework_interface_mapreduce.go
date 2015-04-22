package taskgraph

import (
	"log"

	"./filesystem"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
)

// This interface is used by application during taskgraph configuration phase.
type MapreduceBootstrap interface {
	// These allow application developer to set the task configuration so framework
	// implementation knows which task to invoke at each node.
	SetTaskBuilder(taskBuilder MapreduceTaskBuilder)

	// This allow the application to specify how tasks are connection at each epoch
	SetTopology(topology Topology)

	// After all the configure is done, driver need to call start so that all
	// nodes will get into the event loop to run the application.
	Start()

	// Initialize mapreduce configuration
	InitWithMapreduceConfig(
		mapperNum uint64,
		shuffleNum uint64,
		reducerNum uint64,
		client filesystem.Client,
		outputDirName string,
		outputFileName string,
		mapperFunc func(MapreduceFramework, string),
		reducerFunc func(MapreduceFramework, string, []string),
	)
}

// Framework hides distributed system complexity and provides users convenience of
// high level features.
type MapreduceFramework interface {
	// This allow the task implementation query its neighbors.
	GetTopology() Topology

	// Kill the framework itself.
	// As epoch changes, some nodes isn't needed anymore
	Kill()

	// Some task can inform all participating tasks to shutdown.
	// If successful, all tasks will be gracefully shutdown.
	// TODO: @param status
	ShutdownJob()

	GetLogger() *log.Logger

	// This is used to figure out taskid for current node
	GetTaskID() uint64

	// This is useful for task to inform the framework their status change.
	// metaData has to be really small, since it might be stored in etcd.
	// Set meta flag to notify meta to all nodes of linkType to this node.
	FlagMeta(ctx context.Context, linkType, meta string)

	// Some task can inform all participating tasks to new epoch
	IncEpoch(ctx context.Context)

	// Request data from task toID with specified linkType and meta.
	DataRequest(ctx context.Context, toID uint64, method string, input proto.Message)
	CheckGRPCContext(ctx context.Context) error

	// Mapreduce framework addtional interface
	// This is used for mapper to emit (key, value) pairs
	Emit(key string, value string)

	// This is used for reducer to output their (key, value) result
	Collect(key string, value string)

	GetEpoch() uint64

	// Mapreduce task can write and read file from filesystem
	GetClient() filesystem.Client

	// Mapper task handle data by mapper functiong setting
	GetMapperFunc() func(MapreduceFramework, string)

	// Reducer task handle data by reducer functiong setting
	GetReducerFunc() func(MapreduceFramework, string, []string)

	// Get filesystem output dir path
	GetOutputDirName() string

	// Get filesystem output file name
	GetOutputFileName() string

	// Mapper task set their own task's output path
	SetMapperOutputWriter() 

	// Reducer task set their own task's output path
	SetReducerOutputWriter()

	// Provide interface to change buffer size
	SetReaderBufferSize(int)
	SetWriterBufferSize(int)
	GetReaderBufferSize() int
	GetWriterBufferSize() int

	// Notify framework mapper(or reducer) process finished, thus it can end buffer stream output
	FinishMapper() 
	FinishReducer()	

	// Clean Function
	Clean(string)
}
