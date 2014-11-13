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

type Framework interface {
	// These two are useful for task to inform the framework their status change.
	// metaData has to be really small, since it might be stored in etcd.
	// Flags and Sends the metaData to partent of the current task.
	FlagParentMetaReady(meta Metadata)
	FlagChildMetaReady(meta Metadata)

	// This allow the task implementation query its neighers.
	GetTopology() Topology

	// Some task can inform all participating tasks to exit.
	Exit()

	// Some task can inform all participating tasks to new epoch
	SetEpoch(epochID uint64)

	GetLogger() log.Logger

	// Request data from parent or children.
	DataRequest(toID uint64, meta Metadata)

	// This allow task implementation to node corresponding to taskID so that
	// it can carry out application dependent communication.
	GetNode(taskID uint64) Node

	// Return true if this node has children
	HasChildren() bool
	HasParents() bool

	// This is used to figure out taskid for current node
	GetTaskID() uint64
}
