package taskgraph

import "golang.org/x/net/context"

// Task is a logic repersentation of a computing unit.
// Each task contain at least one Node.
// Each task has exact one master Node and might have multiple salve Nodes.

// All event handler functions and should be non-blocking.
type Task interface {
	// This is useful to bring the task up to speed from scratch or if it recovers.
	Init(taskID uint64, framework Framework)

	// Task is finished up for exit. Last chance to save some task specific work.
	Exit()

	// Framework tells user task what current epoch is.
	// This give the task an opportunity to cleanup and regroup.
	SetEpoch(ctx context.Context, epoch uint64)

	// The meta/data notifications obey exactly-once semantics. Note that the same
	// meta string will be notified only once even if you flag the meta more than once.
	// ParentMetaReady(ctx Context, parentID uint64, meta string)
	// ChildMetaReady(ctx Context, childID uint64, meta string)

	// This now allow use to use arbirary type instead of Parents/Children.
	MetaReady(ctx context.Context, childID uint64, linkType, meta string)

	// These two should go away, folding into DataRequest.
	ParentDataReady(ctx context.Context, parentID uint64, req string, resp []byte)
	ChildDataReady(ctx context.Context, childID uint64, req string, resp []byte)

	// These are payload for application purpose.
	ServeAsParent(fromID uint64, req string) ([]byte, error)
	ServeAsChild(fromID uint64, req string) ([]byte, error)
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
