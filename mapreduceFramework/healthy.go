package mapreduceFramework

import (
	"time"

	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

var (
	heartbeatInterval = 1 * time.Second
)

func (f *mapreducerFramework) heartbeat() {
	f.globalStop = make(chan struct{})
	go func() {
		err := etcdutil.Heartbeat(f.etcdClient, f.name, f.taskID, heartbeatInterval, f.globalStop)
		if err != nil {
			f.log.Printf("Heartbeat stops with error: %v\n", err)
		}
	}()
}
