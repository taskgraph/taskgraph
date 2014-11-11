package meritop

// These two are useful for task to inform the framework their status change.
// metaData has to be really small, since it might be stored in etcd.
type Framework interface {
	// SetUplinkStutus sends the metaData to the partent of the given task.
	SetUplinkStatus(taskID uint64, metaData []byte)
	// SetDownlinkStutus sends the metaData to the chlidren of the given task.
	SetDownlinkStatus(taskID uint64, metaData []byte)

	// These allow application developer to set the task configuration so framework
	// implementation knows which task to invoke at each node.
	AppendTask(task Task)
	SetTaskConfig(taskConfig TaskConfig)

	// This allow the application
	SetTopology(topology Topology)

	// After all the configure is done, driver need to call start so that all
	// nodes will get into the event loop to run the application.
	Start()

	// Some task can inform all participating tasks to exit.
	Exit()

	// Some task can inform all participating tasks to new epoch
	SetEpoch(epochID uint64)
}
