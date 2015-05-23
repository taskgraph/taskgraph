package bwmf

import (
	"encoding/json"

	"github.com/plutoshe/taskgraph"
)

type BWMFTaskBuilder struct {
	NumOfTasks uint64
	ConfBytes  []byte
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	config := &Config{}
	unmarshErr := json.Unmarshal(tb.ConfBytes, config)
	if unmarshErr != nil {
		panic(unmarshErr)
	}

	client, err := GetFsClient(config)
	if err != nil {
		panic(err)
	}

	return &bwmfTask{
		numOfTasks: tb.NumOfTasks,
		config:     config,
		fsClient:   client,
	}
}
