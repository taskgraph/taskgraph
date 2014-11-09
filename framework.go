package meritop

// These two are useful for task to inform the framework their status change.
// metaData has to be really small, since it might be stored in etcd.
type Framework interface {
	// SendParentStutus sends the metaData to the partent of the given task.
	SendParentStatus(taskID uint64, metaData []byte)
	// SendParentStutus sends the metaData to the chlidren of the given task.
	SendChildStatus(taskID uint64, metaData []byte)
}
