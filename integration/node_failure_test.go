package integration

import (
	"log"
	"testing"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop/controller"
	"github.com/go-distributed/meritop/framework"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

// TestFailedMasterTask checks if a master task failed,
// 1. a new boostrap will be created to take over
// 2. continue what's left;
// 3. finish the job with the same result.
func TestRegressionFailedMaster(t *testing.T) {
	job := "regression_failmaster_test"
	m := etcdutil.StartNewEtcdServer(t, job)
	defer m.Terminate(t)

	etcdURLs := []string{m.URL()}
	numOfTasks := uint64(15)

	// controller start first to setup task directories in etcd
	controller := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks)
	go controller.Start()
	defer controller.Stop()
	time.Sleep(500 * time.Millisecond)

	// We need to set etcd so that nodes know what to do.
	taskBuilder := &framework.SimpleTaskBuilder{
		GDataChan:    make(chan int32, 10),
		FinishChan:   make(chan struct{}),
		NodeProducer: make(chan bool, 1),
		Config: map[string]string{
			"SetEpoch":  "fail",
			"failepoch": "1",
			"faillevel": "100",
		},
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, numOfTasks, taskBuilder)
	}
	if <-taskBuilder.NodeProducer {
		time.Sleep(500 * time.Millisecond)
		taskBuilder.Config = nil
		log.Println("Starting a new node")
		// this time we start a new bootstrap whose task master doesn't fail.
		go drive(t, job, etcdURLs, numOfTasks, taskBuilder)
	}

	// wait for last number to comeback.s
	wantData := []int32{0, 105, 210, 315, 420, 525, 630, 735, 840, 945, 1050}
	getData := make([]int32, framework.NumOfIterations+1)
	for i := uint64(0); i <= framework.NumOfIterations; i++ {
		getData[i] = <-taskBuilder.GDataChan
	}

	for i := range wantData {
		if wantData[i] != getData[i] {
			t.Errorf("#%d: data want = %d, get = %d\n", i, wantData[i], getData[i])
		}
	}
	<-taskBuilder.FinishChan
}
