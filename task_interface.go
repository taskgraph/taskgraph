package taskgraph

// TaskBuilder should be implemented by application developer and used by
// framework implementation to decide which task implementation to be used
// at given node.
type TaskBuilder interface {
	// This method is called once by framework implementation to get the
	// right task implementation for given node.
	Build(taskID uint64) Task
}

type Task interface {
	// numberOfTasks: how many tasks are created for this job.
	// User can use this number to make decision on topology.
	Init(framework Framework, numberOfTasks uint64)
	Exit()

	// Framework tells user task what current epoch is.
	// User can compose a graph using channels and joints here.
	SetEpoch(ctx Context, epoch uint64)
}

// user-implemented data processing/computing unit.
type Joint interface {
	Compute(ins []InboundChannel, outs []OutboundChannel)
}

type Serializable interface {
	Serialize() []byte
}
