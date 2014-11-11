/*
The dummy task is designed for regresion test of meritop framework.
This works with
*/
package meritop

type DummyTask struct {
	framework       Framework
	epochID, taskID uint64
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t DummyTask) Init(taskID uint64, framework Framework, config Config) {
	t.taskID = taskID
	t.framework = framework
}

// Task need to finish up for exit, last chance to save work?
func (t DummyTask) Exit() {}

// These are called by framework implementation so that task implementation can
// reacts to parent or children restart.
func (t DummyTask) ParentRestart(parentID uint64) {}
func (t DummyTask) ChildRestart(childID uint64)   {}

func (t DummyTask) ParentDie(parentID uint64) {}
func (t DummyTask) ChildDie(childID uint64)   {}

// Ideally, we should also have the following:
func (t DummyTask) ParentReady(taskID uint64, data []byte) {
	// Get data from parent, and then make it available for children.
	if t.framework.HasChildren() {
		t.framework.SetReadyForChildren(nil)
	} else {
		t.framework.SetReadyForParent(nil)
	}
}

func (t DummyTask) ChildReady(taskID uint64, data []byte) {
	// Get data from child. When all the data is back, we flag the parent.
	if t.taskID != 0 {
		t.framework.SetReadyForParent(nil)
	}
}

// This give the task an opportunity to cleanup and regroup.
func (t DummyTask) SetEpoch(epochID uint64) {
	t.epochID = epochID
	if t.taskID == 0 {
		// this is master node, it should set the parent ready.
		t.framework.SetReadyForChildren(nil)
	} else {
		// this is client node, it should wait for parent ready then inform
		// its children
	}
}

// Some hooks that need for master slave etc.
func (t DummyTask) BecameMaster(nodeID uint64) {}
func (t DummyTask) BecameSlave(nodeID uint64)  {}

type simpleTaskConfig struct{}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc simpleTaskConfig) GetTask(taskID uint64, tasks []Task) Task {
	return tasks[0]
}

// This is used to show how to drive the network.
func drive() {
	var framework Framework
	var task DummyTask
	var taskConfig simpleTaskConfig
	framework.AppendTask(task)
	framework.SetTaskConfig(taskConfig)
	framework.SetTopology(NewTreeTopology(2, 127))
	framework.Start()
}
