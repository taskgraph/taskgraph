package mapreduce

type ShuffleReducerTopology struct {
	NumOfReducer           uint64
	NumOfShuffle           uint64
	taskID                 uint64
	prefix, suffix, master []uint64
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
func (t *ShuffleReducerTopology) SetTaskID(taskID uint64) {
	t.taskID = taskID
	var numOfPrefix uint64
	var numOfSuffix uint64
	var scopeL uint64 = 0
	var shardQuotient uint64 = 0
	var shardReminder uint64 = 0
	if t.NumOfReducer != 0 {
		shardQuotient, shardReminder = t.NumOfShuffle/t.NumOfReducer, t.NumOfShuffle%t.NumOfReducer
	} else {
		shardReminder = t.NumOfShuffle
	}
	switch {
	case taskID < t.NumOfShuffle:
		numOfPrefix = t.NumOfMapper
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
	t.prefix = make([]uint64, 0, numOfPrefix)
	for index := scopeL; index < scopeL+numOfPrefix; index++ {
		t.prefix = append(t.prefix, index)
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
		numOfSuffix = 1
		scopeL = t.NumOfReducer + t.NumOfShuffle
	default:
		numOfSuffix = 0
	}

	t.suffix = make([]uint64, 0, numOfSuffix)
	for index := scopeL; index < scopeL+numOfSuffix; index++ {
		t.suffix = append(t.suffix, index)
	}

	t.master = make([]uint64, 0, 1)
	if taskID != t.NumOfReducer+t.NumOfShuffle {
		t.master = append(t.master, t.NumOfReducer+t.NumOfShuffle)
	}
}

func (t *ShuffleReducerTopology) GetLinkTypes() []string {
	return []string{"Prefix", "Suffix", "Master"}
}

func (t *ShuffleReducerTopology) GetNeighbors(linkType string, epoch uint64) []uint64 {
	res := make([]uint64, 0)
	switch {
	case linkType == "Prefix":
		res = t.prefix
	case linkType == "Suffix":
		res = t.suffix
	case linkType == "Master":
		res = t.master
	}
	return res
}

// Creates a new topology with given number of mapper, shuffle, and reducer
// This will be called during the task graph configuration.
func NewShuffleReducerTopology(ns, nr uint64) *ShuffleReducerTopology {
	m := &ShuffleReducerTopology{
		NumOfShuffle: ns,
		NumOfReducer: nr,
	}
	return m
}
