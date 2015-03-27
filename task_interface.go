package taskgraph

import (
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

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
	EnterEpoch(ctx context.Context, epoch uint64)

	// The meta/data notifications obey exactly-once semantics. Note that the same
	// meta string will be notified only once even if you flag the meta more than once.
	// TODO: one can also get this from channel.
	MetaReady(ctx context.Context, fromID uint64, linkType, meta string)

	// This is the callback when data from server is ready.
	DataReady(ctx context.Context, fromID uint64, method string, output proto.Message)

	CreateOutputMessage(methodName string) proto.Message
	CreateServer() *grpc.Server
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
