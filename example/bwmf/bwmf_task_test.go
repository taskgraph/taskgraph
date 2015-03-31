package bwmf

import (
	"fmt"
	"log"
	"net"
	"testing"

	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework"
)

func TestIntegration(t *testing.T) {
	taskBuilder := &BWMFTaskBuilder{
		numOfIters:      10,
		numOfTasks:      1,
		pgmSigma:        0.1,
		pgmAlpha:        0.5,
		pgmBeta:         0.1,
		pgmTol:          1e-5,
		blockId:         0,
		rowShardPath:    "rowShardPath",
		columnShardPath: "columnShardPath",
		namenodeAddr:    "namenodeAddr",
		webHdfsAddr:     "webHdfsAddr",
		hdfsUser:        "hdfsUser",
	}

	// task := taskBuilder.GetTask(0)

	bootstrap := framework.NewBootStrap("test", []string{"http://127.0.0.1:4001"}, createListener(), nil)
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(topo.NewFullTopology(1))
	bootstrap.Start()
}

func TestLocalComputation(t *testing.T) {
	fmt.Println("Started.")

}

////// utils

func createListener() net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
