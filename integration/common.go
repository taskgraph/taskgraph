package integration

import (
	"net"
	"testing"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework"
)

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}

// This is used to show how to drive the network.
func drive(t *testing.T, jobName string, etcds []string, ntask uint64, taskBuilder taskgraph.TaskBuilder) {
	bootstrap := framework.NewBootStrap(jobName, etcds, createListener(t), nil)
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(topo.NewTreeTopology(2, ntask))
	bootstrap.Start()
}
