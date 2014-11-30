package etcdutil

import "github.com/coreos/go-etcd/etcd"

func TryOccupyTask(client *etcd.Client, name string, taskID uint64) bool {
	return false
}
