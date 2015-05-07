package mapreduce

import (
	"github.com/taskgraph/taskgraph"
)

type MapreduceTaskBuilder struct {
	Config map[string]interface{}
}

func (t *MapreduceTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &mapreduceTask{
		config: t.Config,
	}
}
