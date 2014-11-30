package integration

import (
	"fmt"
	"net"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/example"
	"github.com/go-distributed/meritop/framework"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

func TestRegressionFramework(t *testing.T) {
	m := etcdutil.MustNewMember(t, "framework_regression_test")
	m.Launch()
	defer m.Terminate(t)
	url := fmt.Sprintf("http://%s", m.ClientListeners[0].Addr().String())

	job := "framework_regression_test"
	etcds := []string{url}
	config := map[string]string{}
	numOfTasks := uint64(15)

	// controller start first to setup task directories in etcd
	controller := meritop.NewController(job, etcd.NewClient([]string{url}), numOfTasks)
	controller.InitEtcdLayout()
	defer controller.DestroyEtcdLayout()

	// We need to set etcd so that nodes know what to do.
	taskBuilder := &example.SimpleTaskBuilder{
		GDataChan:  make(chan int32, 10),
		FinishChan: make(chan struct{}),
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcds, config, numOfTasks, taskBuilder)
	}

	// wait for last number to comeback.
	wantData := []int32{0, 105, 210, 315, 420, 525, 630, 735, 840, 945, 1050}
	getData := make([]int32, example.NumOfIterations+1)
	for i := uint64(0); i <= example.NumOfIterations; i++ {
		getData[i] = <-taskBuilder.GDataChan
	}

	for i := range wantData {
		if wantData[i] != getData[i] {
			t.Errorf("#%d: data want = %d, get = %d\n", i, wantData[i], getData[i])
		}
	}

	<-taskBuilder.FinishChan
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}

// This is used to show how to drive the network.
func drive(t *testing.T, jobName string, etcds []string, config meritop.Config, ntask uint64, taskBuilder meritop.TaskBuilder) {
	bootstrap := framework.NewBootStrap(jobName, etcds, config, createListener(t), nil)
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(example.NewTreeTopology(2, ntask))
	bootstrap.Start()
}
