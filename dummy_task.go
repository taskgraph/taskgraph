/*
The dummy task is designed for regresion test of meritop framework.
This works with
*/
package meritop

import (
	"log"
	"os"
)

// DummyData is used to carry parameter and gradient;
type DummyData struct {
	fromTaskID, toTaskID, epochID, uuID uint64
	value                               float32
	data                                [10]float32
}

func (d DummyData) EpochID() uint64 {
	return d.epochID
}

func (d DummyData) ToTaskID() uint64 {
	return d.toTaskID
}

func (d DummyData) FromTaskID() uint64 {
	return d.fromTaskID
}

func (d DummyData) UUID() uint64 {
	return d.uuID
}

// DummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type DummyMaster struct {
	framework       Framework
	epochID, taskID uint64
	logger          *log.Logger

	param, gradient DummyData
	fromChildren    map[uint64]DummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t DummyMaster) Init(taskID uint64, framework Framework, config Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "DummyMaster:", log.Ldate|log.Ltime|log.Lshortfile)

	// Jump start the taskgraph
	t.framework.SetEpoch(0)
}

// Task need to finish up for exit, last chance to save work?
func (t DummyMaster) Exit() {}

// These are called by framework implementation so that task implementation can
// reacts to parent or children restart.
func (t DummyMaster) ParentRestart(parentID uint64) {}
func (t DummyMaster) ChildRestart(childID uint64)   {}

func (t DummyMaster) ParentDie(parentID uint64) {}
func (t DummyMaster) ChildDie(childID uint64)   {}

// Ideally, we should also have the following:
func (t DummyMaster) ParentMetaReady(taskID uint64, meta Metadata) {}
func (t DummyMaster) ChildMetaReady(taskID uint64, meta Metadata) {
	// Get data from child. When all the data is back, starts the next epoch.
	t.framework.DataRequest(taskID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t DummyMaster) SetEpoch(epochID uint64) {
	t.epochID = epochID
	for i := 0; i < 10; i++ {
		t.param.data[i] = float32(t.epochID)
	}

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]DummyData)
	t.framework.FlagChildMetaReady(t.param)
}

// These are payload rpc for application purpose.
func (t DummyMaster) ServeAsParent(req Metadata) Metadata { return t.param }
func (t DummyMaster) ServeAsChild(reg Metadata) Metadata  { return nil }

func (t DummyMaster) ParentDataReady(req, response Metadata) {}
func (t DummyMaster) ChildDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}

	data, ok := req.(DummyData)
	if !ok {
		t.logger.Fatal("Can't interpret request")
	}
	t.fromChildren[data.FromTaskID()] = data

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetChildren(t.epochID)) {
		// In real ML, we modify the gradient first. But here it is noop.
		t.framework.SetEpoch(t.epochID + 1)
	}
}

// DummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type DummySlave struct {
	framework       Framework
	epochID, taskID uint64
	logger          *log.Logger

	param, gradient DummyData
	fromChildren    map[uint64]DummyData
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t DummySlave) Init(taskID uint64, framework Framework, config Config) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "DummySlave:", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t DummySlave) Exit() {}

// These are called by framework implementation so that task implementation can
// reacts to parent or children restart.
func (t DummySlave) ParentRestart(parentID uint64) {}
func (t DummySlave) ChildRestart(childID uint64)   {}

func (t DummySlave) ParentDie(parentID uint64) {}
func (t DummySlave) ChildDie(childID uint64)   {}

// Ideally, we should also have the following:
func (t DummySlave) ParentMetaReady(taskID uint64, meta Metadata) {
	t.framework.DataRequest(taskID, meta)
}

func (t DummySlave) ChildMetaReady(taskID uint64, meta Metadata) {
	t.framework.DataRequest(taskID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t DummySlave) SetEpoch(epochID uint64) {
	t.epochID = epochID

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]DummyData)
}

// These are payload rpc for application purpose.
func (t DummySlave) ServeAsParent(req Metadata) Metadata {
	return t.param
}
func (t DummySlave) ServeAsChild(reg Metadata) Metadata {
	return t.gradient
}

func (t DummySlave) ParentDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}

	data, ok := req.(DummyData)
	if !ok {
		t.logger.Fatal("Can't interpret request")
	}
	t.param = data

	// We need to carry out local compuation.
	for i := 0; i < 10; i++ {
		t.gradient.data[i] = float32(t.framework.GetTaskID())
	}

	// If this task has children, flag meta so that children can start pull
	// parameter.
	if t.framework.HasChildren() {
		t.framework.FlagChildMetaReady(t.param)
	} else {
		// On leaf node, we can immediately return by and flag parent
		// that this node is ready.
		t.framework.FlagParentMetaReady(t.gradient)
	}
}

func (t DummySlave) ChildDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}
	data, ok := req.(DummyData)
	if !ok {
		t.logger.Fatal("Can't interpret request")
	}
	t.fromChildren[data.FromTaskID()] = data

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetChildren(t.epochID)) {
		// In real ML, we add the gradient first.
		for _, g := range t.fromChildren {
			for i := 0; i < 10; i++ {
				t.gradient.data[i] += g.data[i]
			}
		}

		t.framework.FlagParentMetaReady(t.gradient)
	}
}

type simpleTaskBuilder struct{}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc simpleTaskBuilder) GetTask(taskID uint64) Task {
	if taskID == 0 {
		return DummyMaster{}
	} else {
		return DummySlave{}
	}
}

// This is used to show how to drive the network.
func drive() {
	var framework Bootstrapper
	var taskBuilder simpleTaskBuilder
	framework.SetTaskBuilder(taskBuilder)
	framework.SetTopology(NewTreeTopology(2, 127))
	framework.Start()
}
