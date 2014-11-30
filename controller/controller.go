package controller

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

// This is the controller of a job.
// A job needs controller to setup etcd data layout, request
// cluster containers, etc. to setup framework to run.
type Controller struct {
	name       string
	etcdclient *etcd.Client
	numOfTasks uint64
}

func New(name string, etcd *etcd.Client, numOfTasks uint64) *Controller {
	return &Controller{name, etcd, numOfTasks}
}

func (c *Controller) InitEtcdLayout() (err error) {
	// Initilize the job epoch to 0
	c.etcdclient.Create(etcdutil.MakeJobEpochPath(c.name), "0", 0)

	// initiate etcd data layout
	// currently it creates as many unassigned tasks as task masters.
	for i := uint64(0); i < c.numOfTasks; i++ {
		key := etcdutil.MakeTaskMasterPath(c.name, i)
		if _, err := c.etcdclient.Create(key, "empty", 0); err != nil {
			return err
		}
	}
	return
}

func (c *Controller) DestroyEtcdLayout() (err error) {
	c.etcdclient.Delete("/", true)
	return
}
