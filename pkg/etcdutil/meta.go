package etcdutil

import "github.com/coreos/go-etcd/etcd"

func WatchMeta(c *etcd.Client, taskID uint64, path string, stop chan bool, responseHandler func(*etcd.Response, uint64)) error {
	resp, err := c.Get(path, false, false)
	if err != nil {
		return err
	}
	// Get previous meta. We need to handle it.
	if resp.Node.Value != "" {
		responseHandler(resp, taskID)
	}
	receiver := make(chan *etcd.Response, 1)
	go c.Watch(path, resp.EtcdIndex+1, false, receiver, stop)
	go func(receiver chan *etcd.Response) {
		for resp := range receiver {
			responseHandler(resp, taskID)
		}
	}(receiver)
	return nil
}
