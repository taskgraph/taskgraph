package taskgraph

// Task is a logic repersentation of a computing unit.
// Each task contain at least one Node.
// Each task has exact one master Node and might have multiple salve Nodes.

type Task interface {
	// This is useful to bring the task up to speed from scratch or if it recovers.
	Init(taskID uint64, framework Framework)

	// Task need to finish up for exit, last chance to save work?
	Exit()

	// Framework tells user task what current epoch is.
	// This give the task an opportunity to cleanup and regroup.
	SetEpoch(ctx Context, epoch uint64)

	// NOTE: the meta/data ready notifications follow at-least-once fault
	// tolerance semantics
	ParentMetaReady(ctx Context, parentID uint64, meta string)
	ChildMetaReady(ctx Context, childID uint64, meta string)
	ParentDataReady(ctx Context, parentID uint64, req string, resp []byte)
	ChildDataReady(ctx Context, childID uint64, req string, resp []byte)

	// These are payload for application purpose.
	ServeAsParent(fromID uint64, req string) []byte
	ServeAsChild(fromID uint64, req string) []byte
}

type UpdateLog interface {
	UpdateID()
}

// Backupable is an interface that task need to implement if they want to have
// hot standby copy. This is another can of beans.
type Backupable interface {
	// Some hooks that need for master slave etc.
	BecamePrimary()
	BecameBackup()

	// Framework notify this copy to update. This should be the only way that
	// one update the state of copy.
	Update(log UpdateLog)
}
