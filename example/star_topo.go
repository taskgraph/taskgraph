package example

//The star structure is basically assume that all the task forms a star structure.
//Also the star structure stays the same between epochs.
type StarTopology struct {
	numOfTasks        uint64
	taskID            uint64
	parents, children []uint64
}

// TODO, do we really need to expose this? Ideally after proper construction of StarTopology
// we should not need to set this again.
func (t *StarTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID
	// Not the most efficient way to create parents and children, but
	// since this is not on critical path, we are ok.
	if taskID == 0 {
		t.parents = make([]uint64, 0, 0)
		t.children = make([]uint64, 0, t.numOfTasks)
		for index := uint64(1); index < t.numOfTasks; index++ {
			t.children = append(t.children, index)
		}
	} else {
		t.parents = make([]uint64, 0, 1)
		t.children = make([]uint64, 0, 0)
	}

}

func (t *StarTopology) GetParents(epoch uint64) []uint64 { return t.parents }

func (t *StarTopology) GetChildren(epoch uint64) []uint64 { return t.children }

// TODO, do we really need to expose this?
func (t *StarTopology) SetNumberOfTasks(nt uint64) { t.numOfTasks = nt }

// Creates a new tree topology with given fanout and number of tasks.
// This will be called during the task graph configuration.
func NewStarTopology(nTasks uint64) *StarTopology {
	m := &StarTopology{
		numOfTasks: nTasks,
	}
	return m
}
