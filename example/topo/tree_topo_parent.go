package topo

//The tree structure is basically assume that all the task forms a tree.
//Also the tree structure stays the same between epochs.
type TreeTopologyOfParents struct {
	fanout, numOfTasks uint64
	taskID             uint64
	neighbor           []uint64
}

func (t *TreeTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID
	// Not the most efficient way to create parents and children, but
	// since this is not on critical path, we are ok.
	t.parents = make([]uint64, 0, 1)

	for index := uint64(1); index < t.numOfTasks; index++ {
		parentID := (index - 1) / t.fanout
		if index == taskID {
			t.parents = append(t.parents, parentID)
			if len(t.parents) > 1 {
				panic("unexpcted number of partents for a tree topology")
			}
		}
	}
}

func (t *TreeTopology) GetNeighbors(linkType string, epoch uint64) []uint64 {
	return t.parents
}

// Creates a new tree topology with given fanout and number of tasks.
// This will be called during the task graph configuration.
func NewTreeTopologyOfParents(fanout, nTasks uint64) *TreeTopology {
	m := &TreeTopology{
		fanout:     fanout,
		numOfTasks: nTasks,
	}
	return m
}
