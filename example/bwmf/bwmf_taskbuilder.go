package bwmf

import (
	"encoding/json"
	"fmt"

	"github.com/plutoshe/taskgraph"
	"github.com/taskgraph/taskgraph/filesystem"
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

	var client filesystem.Client
	var cltErr error
	switch config.IOConf.Fs {
	case "local":
		client = filesystem.NewLocalFSClient()
	case "hdfs":
		client, cltErr = filesystem.NewHdfsClient(
			config.IOConf.HdfsConf.NamenodeAddr,
			config.IOConf.HdfsConf.WebHdfsAddr,
			config.IOConf.HdfsConf.User,
		)
		if cltErr != nil {
			panic(cltErr)
		}
	case "azure":
		client, cltErr = filesystem.NewAzureClient(
			config.IOConf.AzureConf.AccountName,
			config.IOConf.AzureConf.AccountKey,
			config.IOConf.AzureConf.BlogServiceBaseUrl,
			config.IOConf.AzureConf.ApiVersion,
			config.IOConf.AzureConf.UseHttps,
		)
		if cltErr != nil {
			panic(cltErr)
		}
	default:
		panic(fmt.Errorf("Unknow fs: %s", config.IOConf.Fs))
	}

	return &bwmfTask{
		numOfTasks: tb.NumOfTasks,
		config:     config,
		fsClient:   client,
	}
}
