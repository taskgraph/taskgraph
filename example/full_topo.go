package example

//The full structure is basically assume that every one is also parent for every else.
//And everyone else is communicating to get the data they need.
//
//Also the star structure stays the same between epochs.
type FullTopology struct {
	numOfTasks        uint64
	taskID            uint64
	parents, children []uint64
}

// TODO, do we really need to expose this? Ideally after proper construction of StarTopology
// we should not need to set this again.
func (t *FullTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID

	// each task is parent of every else.
	t.parents = make([]uint64, 0, t.numOfTasks-1)
	for index := uint64(1); index < t.numOfTasks; index++ {
		if index != t.taskID {
			t.parents = append(t.parents, index)
		}
	}

	t.children = make([]uint64, 0, t.numOfTasks-1)
	for index := uint64(1); index < t.numOfTasks; index++ {
		if index != t.taskID {
			t.children = append(t.children, index)
		}
	}

}

func (t *FullTopology) GetParents(epoch uint64) []uint64 { return t.parents }

func (t *FullTopology) GetChildren(epoch uint64) []uint64 { return t.children }

// TODO, do we really need to expose this?
func (t *FullTopology) SetNumberOfTasks(nt uint64) { t.numOfTasks = nt }

// Creates a new tree topology with given fanout and number of tasks.
// This will be called during the task graph configuration.
func NewFullTopology(nTasks uint64) *FullTopology {
	m := &FullTopology{
		numOfTasks: nTasks,
	}
	return m
}
