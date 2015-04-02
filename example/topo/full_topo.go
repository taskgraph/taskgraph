package topo

//The full structure is basically assume that every one is also parent for every else.
//And everyone else is communicating to get the data they need.
//
//Also the star structure stays the same between epochs.
type FullTopology struct {
	numOfTasks uint64
	taskID     uint64
	neighbors  []uint64
}

// TODO, do we really need to expose this? Ideally after proper construction of StarTopology
// we should not need to set this again.
func (t *FullTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID
	t.neighbors = make([]uint64, 0, t.numOfTasks-1)
	for index := uint64(0); index < t.numOfTasks; index++ {
		if index != t.taskID {
			t.neighbors = append(t.neighbors, index)
		}
	}
}

func (t *FullTopology) GetLinkTypes() []string {
	return []string{"Neighbors"}
}

func (t *FullTopology) GetNeighbors(linkType string, epoch uint64) []uint64 {
	res := make([]uint64, 0)
	switch {
	case linkType == "Neighbors":
		res = t.neighbors
	}
	return res
}

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
