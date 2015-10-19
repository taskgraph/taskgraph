package etcdutil

import (
	"log"
	"math/rand"
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
		if resp.Action != "expire" {
			continue
		}
		if err := ReportFailure(client, name, path.Base(resp.Node.Key)); err != nil {
			return err
		}
	}
	return nil
}

// report failure to etcd cluster
// If a framework detects a failure, it tries to report failure to /FreeTasks/{taskID}
func ReportFailure(client *etcd.Client, name, failedTask string) error {
	_, err := client.Set(FreeTaskPath(name, failedTask), "failed", 0)
	return err
}

// WaitFreeTask blocks until it gets a hint of free task
func WaitFreeTask(client *etcd.Client, name string, logger *log.Logger) (uint64, error) {
	slots, err := client.Get(FreeTaskDir(name), false, true)
	if err != nil {
		return 0, err
	}
	if total := len(slots.Node.Nodes); total > 0 {
		ri := rand.Intn(total)
		s := slots.Node.Nodes[ri]
		idStr := path.Base(s.Key)
		id, err := strconv.ParseUint(idStr, 0, 64)
		if err != nil {
			return 0, err
		}
		logger.Printf("got free task %v, randomly choose %d to try...", ListKeys(slots.Node.Nodes), ri)
		return id, nil
	}

	watchIndex := slots.EtcdIndex + 1
	respChan := make(chan *etcd.Response, 1)
	go func() {
		for {
			logger.Printf("start to wait failure at index %d", watchIndex)
			resp, err := client.Watch(FreeTaskDir(name), watchIndex, true, nil, nil)
			if err != nil {
				logger.Printf("WARN: WaitFailure watch failed: %v", err)
				return
			}
			if resp.Action == "set" {
				respChan <- resp
				return
			}
			watchIndex = resp.EtcdIndex + 1
		}
	}()
	var resp *etcd.Response
	var waitTime uint64 = 0
	for {
		select {
		case resp = <-respChan:
			idStr := path.Base(resp.Node.Key)
			id, err := strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				return 0, err
			}
			return id, nil
		case <-time.After(10 * time.Second):
			waitTime++
			logger.Printf("Node already wait failure for %d0s", waitTime)
		}
	}

}

func computeTTL(interval time.Duration) uint64 {
	if interval/time.Second < 1 {
		return 3
	}
	return 3 * uint64(interval/time.Second)
}
