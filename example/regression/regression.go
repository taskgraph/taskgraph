package regression

import "github.com/taskgraph/taskgraph"

type regressionTaskBuilder struct {
}

func (tb *regressionTaskBuilder) Build(taskID uint64) taskgraph.Task {
	if taskID == 0 {
		return &masterTask{}
	}
	return &slaveTask{}
}
