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

	tb := &bwmf.BWMFTaskBuilder{
		NumOfTasks: numOfTasks,
		NumIters:   4,
		ConfBytes:  []byte(`{"OptConf":{"Sigma":0.01,"Alpha":1,"Beta":0.1,"GradTol":1e-06},"IOConf":{"IFs":"local","IDPath":"../example/bwmf/data/row_shard.dat","ITPath":"../example/bwmf/data/column_shard.dat","OFs":"local","ODPath":"../example/bwmf/data/dShard.dat","OTPath":"../example/bwmf/data/tShard.dat","HdfsConf":{}}}`),
		LatentDim:  2,
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, tb, topo.NewFullTopology(numOfTasks))
	}

	ctl.WaitForJobDone()
	ctl.Stop()
}
