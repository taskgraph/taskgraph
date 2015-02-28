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

type Composer interface {
	Compose()
	SetProcessor(Processor)
	AttachInboundChannel(InboundChannel)
	AttachOutboundChannel(InboundChannel)
}
