package taskgraph

import (
	"log"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// This interface is used by application during taskgraph configuration phase.
type Bootstrap interface {
	// These allow application developer to set the task configuration so framework
	// implementation knows which task to invoke at each node.
	SetTaskBuilder(taskBuilder TaskBuilder)

	// This allow the application to specify how tasks are connection at each epoch
	SetTopology(topology Topology)

	// After all the configure is done, driver need to call start so that all
	// nodes will get into the event loop to run the application.
	Start()
}

// Framework hides distributed system complexity and provides users convenience of
// high level features.
type Framework interface {
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

	// ctx -- the Google context library that framework gives to user in task callback
	// and user should pass back in any framework call.
	// taskID -- the task to ask data from.
	// methodName -- the gRPC method to call for serving the data
	// input -- user defined arguments to call gRPC service of the other end.
	// dataChan -- the channel to return requested data.
	Fetch(ctx context.Context, toID uint64, methodName string, input proto.Message, outputC chan<- proto.Message, opts ...grpc.CallOption)
}

// Note that framework can decide how update can be done, and how to serve the updatelog.
type BackedUpFramework interface {
	// Ask framework to do update on this update on this task, which consists
	// of one primary and some backup copies.
	Update(taskID uint64, log UpdateLog)
}
