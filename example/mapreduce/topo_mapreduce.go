package mapreduce

type MapReduceTopology struct {
	NumOfMapper                   uint64
	NumOfShuffle                  uint64
	NumOfReducer                  uint64
	NumOfSlave                    uint64
	taskID                        uint64
	prefix, suffix, master, slave [][]uint64
}

// The mapreduce topo splits into three layer
// Layer 1 :
// Mapper Layer
// Layer 2 :
// Shuffle Layer depends on all Mapper Nodes
// Layer 3 :
// Reducer Layer
// Shuffle Layer divide fairly to every Reducer node
// Prefix and Suffix array represents the dependency relationship between layers
func (t *MapReduceTopology) SetTaskID(taskID uint64) {

	// set topology of epoch 0
	t.prefix = make([][]uint64, 0, 2)
	t.suffix = make([][]uint64, 0, 2)
	t.master = make([][]uint64, 0, 2)
	t.slave = make([][]uint64, 0, 2)

	t.taskID = taskID
	var numOfPrefix uint64
	var numOfSuffix uint64
	var scopeL uint64
	switch {
	// set prefix configuration for master
	case taskID == 0:
		numOfPrefix = 0
		scopeL = 0
	//set prefix configuration for mapper
	case taskID <= t.NumOfMapper:
		numOfPrefix = 0
		scopeL = 0
	//set prefix configuration for shuffle
	case taskID <= t.NumOfMapper+t.NumOfShuffle:
		numOfPrefix = t.NumOfMapper
		scopeL = 1
	default:
		numOfPrefix = 0
		scopeL = 0
	}

	prefix := make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		prefix = append(prefix, index)
	}

	switch {
	// set suffix configuration for master
	case taskID == 0:
		numOfPrefix = 0
		scopeL = 0
	//set suffix configuration for mapper
	case taskID <= t.NumOfMapper:
		numOfSuffix = t.NumOfShuffle
		scopeL = t.NumOfMapper + 1
	//set suffix configuration for shuffle
	case taskID <= t.NumOfMapper+t.NumOfShuffle:
		numOfSuffix = 0
	default:
		numOfSuffix = 0
	}

	suffix := make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		suffix = append(suffix, index)
	}

	master := make([]uint64, 0, 1)
	slave := make([]uint64, 0, t.NumOfSlave)
	if taskID != 0 {
		master = append(master, 0)
	} else {
		for index := uint64(1); index <= t.NumOfSlave; index++ {
			slave = append(slave, index)
		}
	}

	t.prefix = append(t.prefix, prefix)
	t.suffix = append(t.suffix, suffix)
	t.master = append(t.master, master)
	t.slave = append(t.slave, slave)

	// set topology of epoch 0
	switch {
	// set prefix configuration for master
	case taskID == 0:
		numOfPrefix = 0
		scopeL = 0
	// set prefix configuration for shuffle
	case taskID <= t.NumOfShuffle:
		numOfPrefix = 0
		scopeL = 0
	// set prefix configuration for reducer
	case taskID <= t.NumOfShuffle+t.NumOfReducer:
		numOfPrefix = t.NumOfShuffle
		scopeL = 1
	default:
		numOfPrefix = 0
	}

	prefix = make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		prefix = append(prefix, index)
	}

	switch {
	// set suffix configuration for master
	case taskID == 0:
		numOfSuffix = 0
		scopeL = 0
	case taskID <= t.NumOfShuffle:
		numOfSuffix = 0
		scopeL = t.NumOfShuffle + 1
	case taskID <= t.NumOfShuffle+t.NumOfReducer:
		numOfSuffix = 0
		scopeL = 0
	default:
		numOfSuffix = 0
	}

	suffix = make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		suffix = append(suffix, index)
	}

	master = make([]uint64, 0, 1)
	slave = make([]uint64, 0, t.NumOfSlave)
	if taskID != 0 {
		master = append(master, 0)
	} else {
		for index := uint64(1); index <= t.NumOfSlave; index++ {
			slave = append(slave, index)
		}
	}

	t.prefix = append(t.prefix, prefix)
	t.suffix = append(t.suffix, suffix)
	t.master = append(t.master, master)
	t.slave = append(t.slave, slave)

}

func (t *MapReduceTopology) GetLinkTypes() []string {
	return []string{"Master", "Slave", "Prefix", "Suffix"}
}

func (t *MapReduceTopology) GetNeighbors(linkType string, epoch uint64) []uint64 {
	res := make([]uint64, 0)
	switch {
	case linkType == "Prefix":
		res = t.prefix[epoch]
	case linkType == "Suffix":
		res = t.suffix[epoch]
	case linkType == "Master":
		res = t.master[epoch]
	case linkType == "Slave":
		res = t.slave[epoch]
	}
	return res
}

// Creates a new topology with given number of mapper, shuffle, and reducer
// This will be called during the task graph configuration.
func NewMapReduceTopology(nm, ns, nr, nslave uint64) *MapReduceTopology {
	m := &MapReduceTopology{
		NumOfMapper:  nm,
		NumOfShuffle: ns,
		NumOfReducer: nr,
		NumOfSlave:   nslave,
	}
	return m
}
