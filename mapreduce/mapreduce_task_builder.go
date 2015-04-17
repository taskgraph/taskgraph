package mapreduce

import (
	"../../taskgraph"
)

type MapreduceTaskBuilder struct {
	MapperNum, ShuffleNum, ReducerNum uint64
	MapperConfig                      []map[string][]string
	ShuffleConfig                     []map[string]string
	ReducerConfig                     []map[string]string
}

func (t *MapreduceTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID < t.MapperNum {
		if int(taskID) < len(t.MapperConfig) {
			return &mapperTask{
				config: t.MapperConfig[taskID],
			}
		} else {
			return &mapperTask{}
		}
	} else if taskID < t.MapperNum + t.ShuffleNum {
		return &shuffleTask{
			// config: t.ShuffleConfig[taskID - t.MapperNum],
		}
	} else if taskID < t.MapperNum + t.ShuffleNum + t.ReducerNum{
		return &reducerTask{
			// config: t.ReducerConfig[taskID - t.MapperNum - t.ShuffleNum],
		}
	} else {
		return &masterTask{
			mapperNum : t.MapperNum,
			shuffleNum : t.ShuffleNum,
			reducerNum : t.ReducerNum,
		}
	}
}
