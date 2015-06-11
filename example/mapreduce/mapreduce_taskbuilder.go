package mapreduce

import "github.com/taskgraph/taskgraph"

type MapreduceTaskBuilder struct {
	NumOfTasks      uint64
	MapreduceConfig map[string]interface{}
}

func (tb MapreduceTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID == 0 {
		return &masterTask{
			numOfTasks: tb.NumOfTasks,
			config:     MapreduceConfig,
		}
	} else {
		return &workerTask{}
	}
}
