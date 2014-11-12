package meritop

import "log"

// These two are useful for task to inform the framework their status change.
// metaData has to be really small, since it might be stored in etcd.
type Framework interface {
	// Flags and Sends the metaData to partent of the current task.
	FlagReadyForParent(metaData []byte)
	// Flags and Sends the metaData to chlidren of the current task.
	FlagReadyForChildren(metaData []byte)

	// These allow application developer to set the task configuration so framework
	// implementation knows which task to invoke at each node.
	SetTaskBuilder(taskBuilder TaskBuilder)

	// This allow the application
	SetTopology(topology Topology)

	// After all the configure is done, driver need to call start so that all
	// nodes will get into the event loop to run the application.
	Start()

	// Some task can inform all participating tasks to exit.
	Exit()

	// Some task can inform all participating tasks to new epoch
	SetEpoch(epochID uint64)

	GetLogger() log.Logger

	// Request data from parent or children.
	DataRequest(toID uint64, meta TGMeta)

	// This allow task implementation to node corresponding to taskID so that
	// it can carry out application dependent communication.
	GetNode(taskID uint64) Node

	// Return true if this node has children
	HasChildren() bool
	HasParents() bool
}
