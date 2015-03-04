package taskgraph

import "github.com/taskgraph/taskgraph/job"

// This interface is used by application to configure task builder and
// do bootstrap start.
type Bootstrap interface {
	SetTaskBuilder(TaskBuilder)
	Start()
}

type Framework interface {
	// Kill the framework itself.
	// As epoch changes, some tasks isn't needed anymore.
	Kill()

	// Inform all participating tasks to shutdown.
	// All tasks will be gracefully shutdown.
	ShutdownJob(es job.ExitStatus)

	// TaskID for current node
	TaskID() uint64
}

type Context interface {
	CreateComposer() Composer
}

type Composer interface {
	SetJoint(Joint)
	CreateInboundChannel(taskID uint64, tag string)
	CreateOutboundChannel(taskID uint64, tag string)
}

type channelBasics interface {
	TaskID() uint64
	Tag() string
}

type InboundChannel interface {
	channelBasics
	Get() []byte
}

type OutboundChannel interface {
	channelBasics
	Put(Marshaler)
}

// TODO:
// 1. User proto message (deferred)
// 2. Channel Implemented in gRPC
//   - Inbound has a client. Client gets data and then dispatches to all inbound channels
//     that requested.
//   - Outbound has a server.
// 3. Run regression
