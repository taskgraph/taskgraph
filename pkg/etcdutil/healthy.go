package etcdutil

import (
	"math"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// heartbeat to etcd cluster until stop
func Heartbeat(client *etcd.Client, name string, taskID uint64, interval time.Duration, stop chan struct{}) error {
	for {
		_, err := client.Set(HealthyPath(name, taskID), "health", computeTTL(interval))
		if err != nil {
			return err
		}
		select {
		case <-time.After(interval):
		case <-stop:
			return nil
		}
	}
}

// detect failure of the given taskID
func DetectFailure(client *etcd.Client, name string, taskID uint64, stop chan struct{}) uint64 {
	return 0
}

// report failure to etcd cluster
// If a framework detects a failure, it tries to report failure to /failedTasks/{taskID}
func ReportFailure(client *etcd.Client, name string, taskID uint64) {

}

// WaitFailure blocks until it gets a hint of taks failure
func WaitFailure(client *etcd.Client, name string) uint64 {
	return 1
}

func computeTTL(interval time.Duration) uint64 {
	return uint64(math.Min(5*float64(interval/time.Second), 1))
}
