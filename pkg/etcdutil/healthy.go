package etcdutil

import (
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// heartbeat to etcd cluster until stop
func Heartbeat(client *etcd.Client, name string, taskID uint64, interval time.Duration, stop chan struct{}) {
}

// detect failure of the given taskID
func DetectFailure(client *etcd.Client, name string, taskID uint64, stop chan struct{}) uint64 {
	return 1
}

// report failure to etcd cluster
func ReportFailure(client *etcd.Client, name string, taskID uint64) {

}

// WaitFailure blocks until it gets a hint of taks failure
func WaitFailure(client *etcd.Client, name string) uint64 {
	return 1
}
