package taskgraph

import (
	"log"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
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

	// Request data from task toID with specified linkType and meta.
	DataRequest(ctx context.Context, toID uint64, method string, input proto.Message)
	CheckGRPCContext(ctx context.Context) error
}

// Note that framework can decide how update can be done, and how to serve the updatelog.
type BackedUpFramework interface {
	// Ask framework to do update on this update on this task, which consists
	// of one primary and some backup copies.
	Update(taskID uint64, log UpdateLog)
}

type Bootup interface {
	// Blocking call to run the task until it finishes.
	Start()
}

type GRPCHandlerInterceptor interface {
	// Currently grpc doesn't support interceptor functionality. We need to rely on user
	// to call this at handler implementation.
	// The workflow would be
	//   C:Notify -> S:Intercept -> S:OnNotify
	Intercept(ctx context.Context, method string, input proto.Message) (proto.Message, error)
}

// Master-worker paradigm:
// There are usually one master (we can make it fault tolerance) and many workers.
// Master is responsible for making global decision and assign work to individual workers.
// Startup:
// 1. master should start first.
// 2. workers start with a unique worker ID, and ask/notify the master for assignment.
//
// Why master and what should be done on master?
//   Only master can make global decisions. Master should store the states (initial, updated,
// completed, etc.) of each workers and make decisions when state changes. This is important
// when task restart happens and reset state to "initial".
//
// What about worker?
//   Workers do the actual computation and data flow.
//   The framework keeps track of master address(es) in etcd.

type MasterFrame interface {
	// User can use this interface to simplify sending the messages to worker. By keeping
	// track of workers' states, user can make decisions on logical worker and communicate it
	// using proto messages.
	NotifyWorker(ctx context.Context, workerID uint64, method string, input proto.Message) (proto.Message, error)
	GRPCHandlerInterceptor
}

type WorkerFrame interface {
	// It usually send states, etc. information to master in order to get further decision.
	NotifyMaster(ctx context.Context, input proto.Message) (proto.Message, error)
	// Worker-worker data flow
	DataRequest(ctx context.Context, workerID uint64, method string, input proto.Message) (proto.Message, error)
	GRPCHandlerInterceptor
}
