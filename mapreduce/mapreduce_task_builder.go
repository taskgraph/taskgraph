package mapreduce

import (
	"github.com/taskgraph/taskgraph"
)

type mapreduceTaskBuilder struct {
	MapperConfig map[string]string[]
	ShuffleConfig map[string]string
	ReducerConfig map[string]string
}

func (t mapreduceTaskBuilder) GetTask(taskID unint64) taskgraph.Task {
	if taskID < MapperNum {
		return &mapperTask{
			config : t.MapperConfig
		}
	} else if taskID < MapperNum + ShuffleNum {
		return &shuffleTask{
			config : t.ShuffleConfig
		}
	} else {
		return &reduceTask{
			config : t.ReducerConfig
		}
	}
}