package meritop

/*
The dummy task is designed for regresion test of meritop framework.
This works with
*/

import (
	"encoding/json"
	"errors"
	"log"
	"os"
)

// dummyData is used to carry parameter and gradient;
type dummyData struct {
	value float32
	data  [10]float32
}

// dummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type dummyMaster struct {
	framework     Framework
	epoch, taskID uint64
	logger        *log.Logger

	param, gradient *dummyData
	fromChildren    map[uint64]*dummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummyMaster) Init(taskID uint64, framework Framework, config Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "dummyMaster:", log.Ldate|log.Ltime|log.Lshortfile)

	// Jump start the taskgraph
	t.framework.SetEpoch(0)
}

// Task need to finish up for exit, last chance to save work?
func (t *dummyMaster) Exit() {}

// Ideally, we should also have the following:
func (t *dummyMaster) ParentMetaReady(parentID uint64, meta string) {}
func (t *dummyMaster) ChildMetaReady(childID uint64, meta string) {
	// Get data from child. When all the data is back, starts the next epoch.
	t.framework.DataRequest(childID, "")
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) SetEpoch(epoch uint64) {
	t.epoch = epoch
	for i := 0; i < 10; i++ {
		t.param.data[i] = float32(t.epoch)
	}

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
	t.framework.FlagChildMetaReady("ParamReady")
}

// These are payload rpc for application purpose.
func (t *dummyMaster) ServeAsParent(req string) ([]byte, error) {
	return json.Marshal(t.param)
}
func (t *dummyMaster) ServeAsChild(req string) ([]byte, error) {
	return nil, errors.New("Master shouldn't serve as child")
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
		// In real ML, we modify the gradient first. But here it is noop.
		t.framework.SetEpoch(t.epoch + 1)
	}
}

// dummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type dummySlave struct {
	framework     Framework
	epoch, taskID uint64
	logger        *log.Logger

	param, gradient *dummyData
	fromChildren    map[uint64]*dummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummySlave) Init(taskID uint64, framework Framework, config Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "dummySlave:", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t *dummySlave) Exit() {}

// Ideally, we should also have the following:
func (t *dummySlave) ParentMetaReady(parentID uint64, meta string) {
	t.framework.DataRequest(parentID, "")
}

func (t *dummySlave) ChildMetaReady(childID uint64, meta string) {
	t.framework.DataRequest(childID, "")
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) SetEpoch(epoch uint64) {
	t.epoch = epoch

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
}

// These are payload rpc for application purpose.
func (t *dummySlave) ServeAsParent(req string) ([]byte, error) {
	return json.Marshal(t.param)
}
func (t *dummySlave) ServeAsChild(req string) ([]byte, error) {
	return json.Marshal(t.gradient)
}

func (t *dummySlave) ParentDataReady(parentID uint64, req string, resp []byte) {
	t.param = new(dummyData)
	json.Unmarshal(resp, t.param)

	// We need to carry out local compuation.
	for i := 0; i < 10; i++ {
		t.gradient.data[i] = float32(t.framework.GetTaskID())
	}

	// If this task has children, flag meta so that children can start pull
	// parameter.
	children := t.framework.GetTopology().GetChildren(t.epoch)
	if len(children) != 0 {
		t.framework.FlagChildMetaReady("ParamReady")
	} else {
		// On leaf node, we can immediately return by and flag parent
		// that this node is ready.
		t.framework.FlagParentMetaReady("GradientReady")
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
			for i := 0; i < 10; i++ {
				t.gradient.data[i] += g.data[i]
			}
		}

		t.framework.FlagParentMetaReady("GradientReady")
	}
}

type simpleTaskBuilder struct{}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc simpleTaskBuilder) GetTask(taskID uint64) Task {
	if taskID == 0 {
		return &dummyMaster{}
	} else {
		return &dummySlave{}
	}
}

// This is used to show how to drive the network.
func drive() {
	var bootstrap Bootstrap
	var taskBuilder simpleTaskBuilder
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(NewTreeTopology(2, 127))
	bootstrap.Start()
}
