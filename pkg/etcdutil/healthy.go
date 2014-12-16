package etcdutil

import (
	"fmt"
	"path"
	"strconv"
	"time"

	"github.com/coreos/go-etcd/etcd"
)

// heartbeat to etcd cluster until stop
func Heartbeat(client *etcd.Client, name string, taskID uint64, interval time.Duration, stop chan struct{}) error {
	for {
		_, err := client.Set(TaskHealthyPath(name, taskID), "health", computeTTL(interval))
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
func DetectFailure(client *etcd.Client, name string, stop chan bool) error {
	receiver := make(chan *etcd.Response, 1)
	go client.Watch(HealthyPath(name), 0, true, receiver, stop)
	for resp := range receiver {
		if resp.Action != "expire" && resp.Action != "delete" {
			continue
		}
		ReportFailure(client, name, path.Base(resp.Node.Key))
	}
	return nil
}

// report failure to etcd cluster
// If a framework detects a failure, it tries to report failure to /failedTasks/{taskID}
func ReportFailure(client *etcd.Client, name, failedTask string) error {
	_, err := client.Set(FailedTaskPath(name, failedTask), "failed", 0)
	return err
}

// WaitFailure blocks until it gets a hint of taks failure
func WaitFailure(client *etcd.Client, name string) (uint64, error) {
	slots, err := client.Get(FailedTaskDir(name), false, true)
	if err != nil {
		return 0, err
	}
	for _, s := range slots.Node.Nodes {
		idStr := path.Base(s.Key)
		id, err := strconv.ParseUint(idStr, 0, 64)
		if err != nil {
			return 0, err
		}
		return id, nil
	}

	respChan := make(chan *etcd.Response, 1)
	go func() {
		resp, err := client.Watch(FailedTaskDir(name), 0, true, nil, nil)
		if err != nil {
			return
		}
		respChan <- resp
	}()
	var resp *etcd.Response
	select {
	case resp = <-respChan:
	case <-time.After(10 * time.Second):
		return 0, fmt.Errorf("WaitFailure timeout!")
	}
	idStr := path.Base(resp.Node.Key)
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func computeTTL(interval time.Duration) uint64 {
	if interval/time.Second < 1 {
		return 3
	}
	return 3 * uint64(interval/time.Second)
}
