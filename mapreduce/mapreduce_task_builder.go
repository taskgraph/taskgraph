package mapreduce

import (
	"../../taskgraph"
)

type MapreduceTaskBuilder struct {
	MapperNum, ShuffleNum, ReducerNum uint64
	MapperConfig                      map[string][]string
	ShuffleConfig                     map[string]string
	ReducerConfig                     map[string]string
}

func (t *MapreduceTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID < t.MapperNum {
		return &mapperTask{
			config: t.MapperConfig,
		}
	} else if taskID < t.MapperNum+t.ShuffleNum {
		return &shuffleTask{
			config: t.ShuffleConfig,
		}
	} else {
		return &reducerTask{
			config: t.ReducerConfig,
		}
	}
}
