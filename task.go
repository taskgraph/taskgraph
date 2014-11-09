package meritop

type Task interface {
	// This is useful to bring the task up to speed from scratch or if it recovers.
	Init(config Config)

	// Nodes returns the IDs of the node associated with this task.
	Nodes() []uint64
	Master() uint64
	Slaves() uint64

	// These are called by framework implementation so that task implementation can
	// reacts to parent or children restart.
	ParentRestart(taskID uint64)
	ChildRestart(taskID uint64)

	ParentDie(taskID uint64)
	ChildDie(taskID uint64)

	// Ideally, we should also have the following:
	ParentReady(taskID uint64, data []byte)
	ClientReady(taskID uint64, data []byte)

	// This give the task an opportunity to cleanup and regroup.
	SetEpoch(epochID uint64)

	// Some hooks that need for master slave etc.
	BecameMaster(nodeID uint64)
	BecameSlave(nodeID uint64)
}
