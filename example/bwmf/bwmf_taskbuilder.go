package bwmf

import (
	"encoding/json"

	"github.com/taskgraph/taskgraph"
)

type BWMFTaskBuilder struct {
	NumOfTasks uint64
	NumIters   uint64
	ConfBytes  []byte
	LatentDim  int
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	config := &Config{}
	unmarshErr := json.Unmarshal(tb.ConfBytes, config)
	if unmarshErr != nil {
		panic(unmarshErr)
	}
	return &bwmfTask{
		numOfTasks: tb.NumOfTasks,
		numIters:   tb.NumIters,
		config:     config,
		latentDim:  tb.LatentDim,
	}
}
