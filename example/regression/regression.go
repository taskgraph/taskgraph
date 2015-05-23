package regression

import (
	"math/rand"
	"strconv"

	"github.com/plutoshe/taskgraph"
)

/*
The dummy task is designed for regresion test of taskgraph framework.
This works with tree topology.
The main idea behind the regression test is following:
There will be two kinds of dummyTasks: master and slaves. We will have one master
sits at the top with taskID = 0, and then rest 6 (2^n - 2) tasks forms a tree under
the master. There will be 10 epochs, from 1 to 10, at each epoch, we send out a
vector with all values equal to epochID, and each slave is supposedly return a vector
with all values equals epochID*taskID, the values are reduced back to master, and
master will print out the epochID and aggregated vector. After all 10 epoch, it kills
job.
*/

// used for testing
type SimpleTaskBuilder struct {
	GDataChan          chan int32
	NumberOfIterations uint64
	NodeProducer       chan bool
	MasterConfig       map[string]string
	SlaveConfig        map[string]string
}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc SimpleTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID == 0 {
		return &dummyMaster{
			dataChan:           tc.GDataChan,
			NodeProducer:       tc.NodeProducer,
			config:             tc.MasterConfig,
			numberOfIterations: tc.NumberOfIterations,
		}
	}
	return &dummySlave{
		NodeProducer: tc.NodeProducer,
		config:       tc.SlaveConfig,
	}
}

func probablyFail(levelStr string) bool {
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return false
	}
	if level < rand.Intn(100)+1 {
		return false
	}
	return true
}
