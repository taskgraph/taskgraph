package topo

//The full structure is basically assume that every one is also parent for every else.
//And everyone else is communicating to get the data they need. Task 0 is forced/assumed to be the master.
//
//Also the star structure stays the same between epochs.
type FullTopology struct {
	numOfTasks uint64
	taskID     uint64
	all        []uint64
}

// TODO, do we really need to expose this? Ideally after proper construction of StarTopology
// we should not need to set this again.
func (t *FullTopology) SetTaskID(taskID uint64) {
	t.all = make([]uint64, 0, t.numOfTasks)
	t.taskID = taskID
	for index := uint64(0); index < t.numOfTasks; index++ {
		t.all = append(t.all, index)
	}
}

func (t *FullTopology) GetLinkTypes() []string {
	return []string{"Neighbors", "Master"}
}

func (t *FullTopology) GetNeighbors(linkType string, epoch uint64) (res []uint64) {
	switch linkType {
	case "Neighbors":
		res = t.all
	case "Master":
		res = []uint64{0}
	default:
		panic("")
	}
	return
}

// Creates a new tree topology with given fanout and number of tasks.
// This will be called during the task graph configuration.
func NewFullTopology(nTasks uint64) *FullTopology {
	return &FullTopology{
		numOfTasks: nTasks,
	}
}
