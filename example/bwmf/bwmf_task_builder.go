package bwmf

import (
	"github.com/taskgraph/taskgraph"
)

// TaskBuilder with task specific configs.
type BWMFTaskBuilder struct {
	numOfTasks uint64
	numOfIters uint64
	pgmSigma   float32
	pgmAlpha   float32
	pgmBeta    float32
	pgmTol     float32
	blockId    uint32
	K          int

	rowShardPath, columnShardPath string

	namenodeAddr, webHdfsAddr, hdfsUser string
}

func (btb *BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{
		numOfIters:      btb.numOfIters,
		numOfTasks:      btb.numOfTasks,
		sigma:           btb.pgmSigma,
		alpha:           btb.pgmAlpha,
		beta:            btb.pgmBeta,
		tol:             btb.pgmTol,
		blockId:         btb.blockId,
		k:               btb.K,
		rowShardPath:    btb.rowShardPath,
		columnShardPath: btb.columnShardPath,
		namenodeAddr:    btb.namenodeAddr,
		webHdfsAddr:     btb.webHdfsAddr,
		hdfsUser:        btb.hdfsUser,
	}
}
