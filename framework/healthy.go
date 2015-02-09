package framework

import (
	"time"

	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

var (
	heartbeatInterval = 1 * time.Second
)

func (f *framework) heartbeat() {
	f.heartbeatStop = make(chan struct{})
	go func() {
		err := etcdutil.Heartbeat(f.etcdClient, f.name, f.taskID, heartbeatInterval, f.heartbeatStop)
		if err != nil {
			f.log.Printf("Heartbeat stops with error: %v\n", err)
		}
	}()
}
