package framework

import (
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
)

type worker struct {
	id         uint64
	job        string
	etcdURL    []string
	listener   net.Listener
	logger     *log.Logger
	task       taskgraph.WorkerTask
	etcdClient *etcd.Client
	stopChan   chan struct{}
}

func NewWorkerBoot(job string, etcdURL []string, ln net.Listener, logger *log.Logger, task taskgraph.WorkerTask, id uint64) taskgraph.Bootup {
	return &worker{
		job:      job,
		etcdURL:  etcdURL,
		listener: ln,
		logger:   logger,
		task:     task,
		id:       id,
	}
}
