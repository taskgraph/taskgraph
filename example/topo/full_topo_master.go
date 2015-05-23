package topo

//The full structure is basically assume that every one is also parent for every else.
//And everyone else is communicating to get the data they need. Task 0 is forced/assumed to be the master.
//
//Also the star structure stays the same between epochs.
type FullTopologyOfMaster struct {
	numOfTasks uint64
	taskID     uint64
	all        []uint64
}

// TODO, do we really need to expose this? Ideally after proper construction of StarTopology
// we should not need to set this again.
func (t *FullTopologyOfMaster) SetTaskID(taskID uint64) {
	t.all = make([]uint64, 0, t.numOfTasks)
	t.taskID = taskID
	for index := uint64(0); index < t.numOfTasks; index++ {
		t.all = append(t.all, index)
	}
}

func (t *FullTopologyOfMaster) GetNeighbors(epoch uint64) []uint64 {
	res := []uint64{0}
	return res
}

// Creates a new tree topology with given fanout and number of tasks.
// This will be called during the task graph configuration.
func NewFullTopologyOfMaster(nTasks uint64) *FullTopologyOfMaster {
	return &FullTopologyOfMaster{
		numOfTasks: nTasks,
	}
}
