package mapreduce

import (
	"github.com/taskgraph/taskgraph"
)

type mapreduceTaskBuilder struct {

	MapperNum uint64
	ShuffleNum uint64
	ReducerNum uint64
	MapperConfig map[string]string
	ShuffleConfig map[string]string
	ReducerConfig map[string]string
	mapperFunc func(string)
}

func (t mapreduceTaskBuilder) GetTask(taskID unint64) taskgraph.Task {
	if taskID < MapperNum {
		return &mapperTask{

		}
	} else if taskID < MapperNum + ShuffleNum {
		return &shuffleTask{

		}
	} else {
		return &reduceTask{

		}
	}
}