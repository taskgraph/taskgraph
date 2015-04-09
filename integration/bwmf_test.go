package integration

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/bwmf"
	"github.com/taskgraph/taskgraph/example/topo"
)

func TestBWMF(t *testing.T) {
	etcdURLs := []string{"http://localhost:4001"}

	job := "bwmf_basic_test"
	numOfTasks := uint64(2)

	ctl := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks, []string{"Neighbors", "Master"})
	ctl.Start()

	tb := &bwmf.BWMFTaskBuilder{NumOfTasks: numOfTasks}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, tb, topo.NewFullTopology(numOfTasks))
	}

	ctl.WaitForJobDone()
	ctl.Stop()
}
