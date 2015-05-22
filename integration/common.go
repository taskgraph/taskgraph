package integration

import (
	"net"
	"testing"

	"../../taskgraph"
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
func driveWithTreeTopo(t *testing.T, jobName string, etcds []string, ntask uint64, taskBuilder taskgraph.TaskBuilder) {
	drive(
		t, 
		jobName, 
		etcds, 
		taskBuilder, 
		map[string]int{
			"parent" : topo.NewTreeTopologyOfParent(numOfTasks))
			"children" : topo.NewTreeTopologyOfNeighbor(numOfTasks)
		}
	)
}

func drive(t *testing.T, jobName string, etcds []string, taskBuilder taskgraph.TaskBuilder, topo map[string]taskgraph.Topology) {

	bootstrap := framework.NewBootStrap(jobName, etcds, createListener(t), nil)
	bootstrap.SetTaskBuilder(taskBuilder)
	for i, _ := range topo {
		bootstrap.AddLinkage(i, topo[i])
	}
	bootstrap.Start()
}
