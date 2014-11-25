package meritop

import "github.com/coreos/go-etcd/etcd"

// This is the controller of a job.
// A job needs controller to setup etcd data layout, request
// cluster containers, etc. to setup framework to run.
type controller struct {
	name       string
	etcdclient *etcd.Client
	numOfTasks uint64
}

func (c *controller) initEtcdLayout() (err error) {
	// initiate etcd data layout
	// currently it creates as many unassigned tasks as task masters.
	for i := uint64(0); i < c.numOfTasks; i++ {
		key := MakeTaskMasterPath(c.name, i)
		if _, err := c.etcdclient.Create(key, "empty", 0); err != nil {
			return err
		}
	}
	return
}

func (c *controller) destroyEtcdLayout() (err error) {
	c.etcdclient.Delete("/", true)
	return
}
