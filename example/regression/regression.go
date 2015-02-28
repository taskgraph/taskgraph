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

type parameterProcessor struct{}
type gradientProcessor struct{}

func CreateInChannel(from uint64, tag string) taskgraph.InboundChannel {
	panic("")
}
func CreateOutChannel(to uint64, tag string) taskgraph.OutboundChannel {
	panic("")
}
