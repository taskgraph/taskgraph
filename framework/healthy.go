package framework

import (
	"time"

	"github.com/go-distributed/meritop/pkg/etcdutil"
)

var (
	heartbeatInterval = 15 * time.Second
)

func (f *framework) heartbeat() {
	etcdutil.Heartbeat(f.etcdClient, f.name, f.taskID, heartbeatInterval, make(chan struct{}))
}
