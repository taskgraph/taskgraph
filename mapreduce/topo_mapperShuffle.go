package mapreduce

type MapperShuffleTopology struct {
	NumOfMapper                   uint64
	NumOfShuffle                  uint64
	taskID                        uint64
	prefix, suffix, master, slave []uint64
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
func (t *MapperShuffleTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID
	var numOfPrefix uint64
	var numOfSuffix uint64
	var scopeL uint64
	switch {
	case taskID < t.NumOfMapper:
		numOfPrefix = 0
		scopeL = 0
	case taskID < t.NumOfMapper+t.NumOfShuffle:
		numOfPrefix = t.NumOfMapper
		scopeL = 0
	default:
		numOfPrefix = 0
		scopeL = t.NumOfMapper + t.NumOfShuffle
	}
	t.prefix = make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		t.prefix = append(t.prefix, index)
	}

	switch {
	case taskID < t.NumOfMapper:
		numOfSuffix = t.NumOfShuffle
		scopeL = t.NumOfMapper
	case taskID < t.NumOfMapper+t.NumOfShuffle:
		numOfSuffix = 0
	default:
		numOfSuffix = 0
	}

	t.suffix = make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		t.suffix = append(t.suffix, index)
	}

	t.master = make([]uint64, 0, 1)
	t.slave = make([]uint64, 0, t.NumOfMapper+t.NumOfShuffle)
	if taskID != t.NumOfMapper+t.NumOfShuffle {
		t.master = append(t.master, t.NumOfMapper+t.NumOfShuffle)
	} else {
		for index := 0; index < NumOfMapper+t.NumOfShuffle; index++ {
			t.slave = append(t.slave, index)
		}
	}
}

func (t *MapperShuffleTopology) GetLinkTypes() []string {
	return []string{"Master", "Slave", "Prefix", "Suffix"}
}

func (t *MapperShuffleTopology) GetNeighbors(linkType string, epoch uint64) []uint64 {
	res := make([]uint64, 0)
	switch {
	case linkType == "Prefix":
		res = t.prefix
	case linkType == "Suffix":
		res = t.suffix
	case linkType == "Master":
		res = t.master
	case lineType == "Slave":
		res = t.slave
	}
	return res
}

// Creates a new topology with given number of mapper, shuffle, and reducer
// This will be called during the task graph configuration.
func NewMapperShuffleTopology(nm, ns uint64) *MapperShuffleTopology {
	m := &MapperShuffleTopology{
		NumOfMapper:  nm,
		NumOfShuffle: ns,
	}
	return m
}
