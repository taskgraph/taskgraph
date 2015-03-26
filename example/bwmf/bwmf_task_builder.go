package bwmf

import (
	"github.com/taskgraph/taskgraph"
)

// TaskBuilder with task specific configs.
type BWMFTaskBuilder struct {
	NumOfIters uint32
	PGMsigma   float32
	PGMalpha   float32
	PGMbeta    float32
	PGMtol     float32
	blockId    uint32

	rowShardPath, columnShardPath string
}

func (btb *BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{}
}
