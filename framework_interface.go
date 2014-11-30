package meritop

import "log"

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

// Note that framework can decide how update can be done, and how to serve the updatelog.
type BackedUpFramework interface {
	// Ask framework to do update on this update on this task, which consists
	// of one primary and some backup copies.
	Update(taskID uint64, log UpdateLog)
}

// Framework hides distributed system complexity and provides users convenience of
// high level features.
type Framework interface {
	// These two are useful for task to inform the framework their status change.
	// metaData has to be really small, since it might be stored in etcd.
	// Set meta flag to notify parent/child of the change.
	FlagMetaToParent(meta string)
	FlagMetaToChild(meta string)

	// This allow the task implementation query its neighbors.
	GetTopology() Topology

	// Some task can inform all participating tasks to shutdown.
	// If successful, all tasks will be gracefully shutdown.
	ShutdownJob()

	// Some task can inform all participating tasks to new epoch
	IncEpoch()

	GetLogger() *log.Logger

	// Request data from parent or children.
	DataRequest(toID uint64, meta string)

	// This is used to figure out taskid for current node
	GetTaskID() uint64
}
