/*
The dummy task is designed for regresion test of meritop framework.
This works with
*/
package meritop

import (
	"log"
	"os"
)

// dummyData is used to carry parameter and gradient;
type dummyData struct {
	fromTaskID, toTaskID, epochID, uuID uint64
	value                               float32
	data                                [10]float32
}

func (d *dummyData) EpochID() uint64 {
	return d.epochID
}

func (d *dummyData) ToTaskID() uint64 {
	return d.toTaskID
}

func (d *dummyData) FromTaskID() uint64 {
	return d.fromTaskID
}

func (d *dummyData) UUID() uint64 {
	return d.uuID
}

// dummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type dummyMaster struct {
	framework       Framework
	epochID, taskID uint64
	logger          *log.Logger

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

// These are called by framework implementation so that task implementation can
// reacts to parent or children restart.
func (t *dummyMaster) ParentRestart(parentID uint64) {}
func (t *dummyMaster) ChildRestart(childID uint64)   {}

func (t *dummyMaster) ParentDie(parentID uint64) {}
func (t *dummyMaster) ChildDie(childID uint64)   {}

// Ideally, we should also have the following:
func (t *dummyMaster) ParentMetaReady(taskID uint64, meta Metadata) {}
func (t *dummyMaster) ChildMetaReady(taskID uint64, meta Metadata) {
	// Get data from child. When all the data is back, starts the next epoch.
	t.framework.DataRequest(taskID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) SetEpoch(epochID uint64) {
	t.epochID = epochID
	for i := 0; i < 10; i++ {
		t.param.data[i] = float32(t.epochID)
	}

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
	t.framework.FlagChildMetaReady(t.param)
}

// These are payload rpc for application purpose.
func (t *dummyMaster) ServeAsParent(req Metadata) Metadata { return t.param }
func (t *dummyMaster) ServeAsChild(reg Metadata) Metadata  { return nil }

func (t *dummyMaster) ParentDataReady(req, response Metadata) {}
func (t *dummyMaster) ChildDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}

	data, ok := req.(*dummyData)
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

// dummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type dummySlave struct {
	framework       Framework
	epochID, taskID uint64
	logger          *log.Logger

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

// These are called by framework implementation so that task implementation can
// reacts to parent or children restart.
func (t *dummySlave) ParentRestart(parentID uint64) {}
func (t *dummySlave) ChildRestart(childID uint64)   {}

func (t *dummySlave) ParentDie(parentID uint64) {}
func (t *dummySlave) ChildDie(childID uint64)   {}

// Ideally, we should also have the following:
func (t *dummySlave) ParentMetaReady(taskID uint64, meta Metadata) {
	t.framework.DataRequest(taskID, meta)
}

func (t *dummySlave) ChildMetaReady(taskID uint64, meta Metadata) {
	t.framework.DataRequest(taskID, meta)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) SetEpoch(epochID uint64) {
	t.epochID = epochID

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*dummyData)
}

// These are payload rpc for application purpose.
func (t *dummySlave) ServeAsParent(req Metadata) Metadata {
	return t.param
}
func (t *dummySlave) ServeAsChild(reg Metadata) Metadata {
	return t.gradient
}

func (t *dummySlave) ParentDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}

	data, ok := req.(*dummyData)
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

func (t *dummySlave) ChildDataReady(req, response Metadata) {
	if req.EpochID() != t.epochID {
		return
	}
	data, ok := req.(*dummyData)
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
