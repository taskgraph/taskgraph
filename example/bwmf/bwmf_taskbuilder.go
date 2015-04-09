package bwmf

import "github.com/taskgraph/taskgraph"

type BWMFTaskBuilder struct {
	NumOfTasks uint64
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{
		numOfTasks: tb.NumOfTasks,
	}
}
