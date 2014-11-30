package example

import (
	"encoding/json"
	"log"
	"os"

	"github.com/go-distributed/meritop"
)

/*
The dummy task is designed for regresion test of meritop framework.
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

const (
	NumOfIterations uint64 = uint64(10)
)

// dummyData is used to carry parameter and gradient;
type dummyData struct {
	Value int32
}

// dummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type dummyMaster struct {
	dataChan      chan int32
	finishChan    chan struct{}
	framework     meritop.Framework
	epoch, taskID uint64
	logger        *log.Logger

	param, gradient *dummyData
	fromChildren    map[uint64]*dummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummyMaster) Init(taskID uint64, framework meritop.Framework, config meritop.Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t *dummyMaster) Exit() {}

// Ideally, we should also have the following:
func (t *dummyMaster) ParentMetaReady(parentID uint64, meta string) {}
func (t *dummyMaster) ChildMetaReady(childID uint64, meta string) {
	// Get data from child. When all the data is back, starts the next epoch.
	t.framework.DataRequest(childID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) SetEpoch(epoch uint64) {
	t.param = &dummyData{}
	t.gradient = &dummyData{}

	t.epoch = epoch
	t.param.Value = int32(t.epoch)

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
	t.framework.FlagMetaToChild("ParamReady")
}

// These are payload rpc for application purpose.
func (t *dummyMaster) ServeAsParent(fromID uint64, req string) []byte {
	b, err := json.Marshal(t.param)
	if err != nil {
		t.logger.Fatalf("Master can't encode parameter: %v, error: %v\n", t.param, err)
	}
	return b
}

func (t *dummyMaster) ServeAsChild(fromID uint64, req string) []byte {
	return nil
}

func (t *dummyMaster) ParentDataReady(parentID uint64, req string, resp []byte) {}
func (t *dummyMaster) ChildDataReady(childID uint64, req string, resp []byte) {
	d := new(dummyData)
	json.Unmarshal(resp, d)
	t.fromChildren[childID] = d

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetChildren(t.epoch)) {
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}

		t.dataChan <- t.gradient.Value
		// TODO(xiaoyunwu) We need to do some test here.

		// In real ML, we modify the gradient first. But here it is noop.
		// Notice that we only
		if t.epoch == NumOfIterations {
			t.framework.ShutdownJob()
			close(t.finishChan)
		} else {
			t.framework.IncEpoch()
		}
	}
}

// dummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type dummySlave struct {
	framework     meritop.Framework
	epoch, taskID uint64
	logger        *log.Logger

	param, gradient *dummyData
	fromChildren    map[uint64]*dummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummySlave) Init(taskID uint64, framework meritop.Framework, config meritop.Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t *dummySlave) Exit() {}

// Ideally, we should also have the following:
func (t *dummySlave) ParentMetaReady(parentID uint64, meta string) {
	t.framework.DataRequest(parentID, meta)
}

func (t *dummySlave) ChildMetaReady(childID uint64, meta string) {
	t.framework.DataRequest(childID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) SetEpoch(epoch uint64) {
	t.param = &dummyData{}
	t.gradient = &dummyData{}

	t.epoch = epoch
	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
}

// These are payload rpc for application purpose.
func (t *dummySlave) ServeAsParent(fromID uint64, req string) []byte {
	b, err := json.Marshal(t.param)
	if err != nil {
		t.logger.Fatalf("Slave can't encode parameter: %v, error: %v\n", t.param, err)
	}
	return b
}

func (t *dummySlave) ServeAsChild(fromID uint64, req string) []byte {
	b, err := json.Marshal(t.gradient)
	if err != nil {
		t.logger.Fatalf("Slave can't encode gradient: %v, error: %v\n", t.gradient, err)
	}
	return b
}

func (t *dummySlave) ParentDataReady(parentID uint64, req string, resp []byte) {
	t.param = new(dummyData)
	json.Unmarshal(resp, t.param)

	// We need to carry out local compuation.
	t.gradient.Value = t.param.Value * int32(t.framework.GetTaskID())

	// If this task has children, flag meta so that children can start pull
	// parameter.
	children := t.framework.GetTopology().GetChildren(t.epoch)
	if len(children) != 0 {
		t.framework.FlagMetaToChild("ParamReady")
	} else {
		// On leaf node, we can immediately return by and flag parent
		// that this node is ready.
		t.framework.FlagMetaToParent("GradientReady")
	}
}

func (t *dummySlave) ChildDataReady(childID uint64, req string, resp []byte) {
	t.fromChildren[childID] = new(dummyData)
	json.Unmarshal(resp, t.fromChildren[childID])

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetChildren(t.epoch)) {
		// In real ML, we add the gradient first.
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}

		t.framework.FlagMetaToParent("GradientReady")
	}
}

type SimpleTaskBuilder struct {
	GDataChan  chan int32
	FinishChan chan struct{}
}

// Leave it at global level so that we use this to terminate and test.
// cDataChan := make(chan *tDataBundle, 1)

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc SimpleTaskBuilder) GetTask(taskID uint64) meritop.Task {
	if taskID == 0 {
		return &dummyMaster{dataChan: tc.GDataChan, finishChan: tc.FinishChan}
	}
	return &dummySlave{}
}
