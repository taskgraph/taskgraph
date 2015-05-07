package framework

import (
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
)

type master struct {
	job        string
	etcdURL    []string
	listener   net.Listener
	logger     *log.Logger
	task       taskgraph.MasterTask
	workerNum  uint64
	etcdClient *etcd.Client
	stopChan   chan struct{}
}

func NewMasterBoot(job string, etcdURL []string, ln net.Listener, logger *log.Logger, task taskgraph.MasterTask, workerNum uint64) taskgraph.Bootup {
	return &master{
		job:       job,
		etcdURL:   etcdURL,
		listener:  ln,
		logger:    logger,
		task:      task,
		workerNum: workerNum,
	}
}
