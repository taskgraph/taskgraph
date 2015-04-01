package bwmf

import (
	"github.com/taskgraph/taskgraph"
)

// TaskBuilder with task specific configs.
type BWMFTaskBuilder struct {
	NumOfTasks     uint64
	NumOfIters     uint64
	PgmSigma       float32
	PgmAlpha       float32
	PgmBeta        float32
	PgmTol         float32
	BlockId        uint32
	K              int
	RowShardBuf    []byte
	ColumnShardBuf []byte
}

func (btb *BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{
		numOfIters: btb.NumOfIters,
		numOfTasks: btb.NumOfTasks,
		sigma:      btb.PgmSigma,
		alpha:      btb.PgmAlpha,
		beta:       btb.PgmBeta,
		tol:        btb.PgmTol,
		blockId:    btb.BlockId,
		k:          btb.K,
		rowBuf:     btb.RowShardBuf,
		columnBuf:  btb.ColumnShardBuf,
	}
}
