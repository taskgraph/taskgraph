package integration

import (
	"net"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/regression"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework"
)

func TestRegressionFramework(t *testing.T) {
	etcdURLs := []string{"http://localhost:4001"}

	job := "framework_regression_test"
	numOfTasks := uint64(15)
	numOfIterations := uint64(10)

	// controller start first to setup task directories in etcd
	controller := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks, []string{"Parents", "Children"})
	controller.Start()

	// We need to set etcd so that nodes know what to do.
	taskBuilder := &regression.SimpleTaskBuilder{
		GDataChan:          make(chan int32, 11),
		NumberOfIterations: numOfIterations,
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, numOfTasks, taskBuilder)
	}

	wantData := []int32{0, 105, 210, 315, 420, 525, 630, 735, 840, 945, 1050}
	getData := make([]int32, numOfIterations+1)
	for i := uint64(0); i <= numOfIterations; i++ {
		getData[i] = <-taskBuilder.GDataChan
	}
	for i := range wantData {
		if wantData[i] != getData[i] {
			t.Errorf("#%d: data want = %d, get = %d\n", i, wantData[i], getData[i])
		}
	}

	controller.WaitForJobDone()
	controller.Stop()
}

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
