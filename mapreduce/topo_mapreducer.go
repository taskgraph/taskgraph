package mapreduce

type MapReduceTopology struct {
	NumOfMapper                   uint64
	NumOfShuffle                  uint64
	NumOfReducer                  uint64
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
	//epoch 0
	t.prefix = make([][]uint64, 0, 2)
	t.suffix = make([][]uint64, 0, 2)
	t.master = make([][]uint64, 0, 2)
	t.slave = make([][]uint64, 0, 2)
	GlobalSlaveNum := t.NumOfReducer + t.NumOfShuffle
	if t.NumOfMapper+t.NumOfShuffle > GlobalSlaveNum {
		GlobalSlaveNum = t.NumOfMapper + t.NumOfShuffle
	}

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

	prefix := make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		prefix = append(prefix, index)
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

	suffix := make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		suffix = append(suffix, index)
	}

	master := make([]uint64, 0, 1)
	slave := make([]uint64, 0, GlobalSlaveNum)
	if taskID != t.NumOfMapper+t.NumOfShuffle {
		master = append(master, GlobalSlaveNum)
	} else {
		for index := uint64(0); index < GlobalSlaveNum; index++ {
			slave = append(slave, index)
		}
	}

	t.prefix = append(t.prefix, prefix)
	t.suffix = append(t.suffix, suffix)
	t.master = append(t.master, master)
	t.slave = append(t.slave, slave)

	//epoch 1
	var shardQuotient uint64 = 0
	var shardReminder uint64 = 0
	if t.NumOfReducer != 0 {
		shardQuotient, shardReminder = t.NumOfShuffle/t.NumOfReducer, t.NumOfShuffle%t.NumOfReducer
	} else {
		shardReminder = t.NumOfShuffle
	}
	switch {
	case taskID < t.NumOfShuffle:
		numOfPrefix = 0
		scopeL = 0
	case taskID < t.NumOfShuffle+shardReminder:
		numOfPrefix = shardQuotient + 1
		scopeL = (shardQuotient + 1) * (taskID - t.NumOfMapper - t.NumOfShuffle)
	case taskID < t.NumOfShuffle+t.NumOfReducer:
		numOfPrefix = t.NumOfShuffle / t.NumOfReducer
		scopeL = t.NumOfShuffle % t.NumOfReducer * (shardQuotient + 1)
		scopeL += (taskID - t.NumOfShuffle - shardReminder) * shardQuotient
	default:
		numOfPrefix = 0
	}

	prefix = make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		prefix = append(prefix, index)
	}

	switch {
	case taskID < t.NumOfShuffle:
		if t.NumOfReducer != 0 {
			numOfSuffix = 1
			tmpAcc := taskID
			if tmpAcc/(shardQuotient+1) < shardReminder {
				scopeL = tmpAcc/(shardQuotient+1) + t.NumOfShuffle
			} else {
				scopeL = tmpAcc - shardReminder*(shardQuotient+1)
				if shardQuotient != 0 {
					scopeL = scopeL / shardQuotient
				}
				scopeL += scopeL + t.NumOfShuffle + shardReminder
			}
		}
	case taskID < t.NumOfShuffle+t.NumOfReducer:
		numOfSuffix = 0
		scopeL = t.NumOfReducer + t.NumOfShuffle
	default:
		numOfSuffix = 0
	}

	suffix = make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		suffix = append(suffix, index)
	}

	master = make([]uint64, 0, 1)
	slave = make([]uint64, 0, GlobalSlaveNum)
	if taskID != t.NumOfReducer+t.NumOfShuffle {
		master = append(master, GlobalSlaveNum)
	} else {
		for index := uint64(0); index < GlobalSlaveNum; index++ {
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
func NewMapReduceTopology(nm, ns, nr uint64) *MapReduceTopology {
	m := &MapReduceTopology{
		NumOfMapper:  nm,
		NumOfShuffle: ns,
		NumOfReducer: nr,
	}
	return m
}
