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

func (f *framework) detectAndReportFailures() {
	failures := make(chan uint64)
	// assume the topo does not change with epoch for now
	// TODO: stop routines...
	for _, id := range f.topology.GetChildren(f.epoch) {
		go func() {
			for {
				failures <- etcdutil.DetectFailure(f.etcdClient, f.name, id, make(chan struct{}))
			}
		}()
	}
	for _, id := range f.topology.GetParents(f.epoch) {
		go func() {
			for {
				failures <- etcdutil.DetectFailure(f.etcdClient, f.name, id, make(chan struct{}))
			}
		}()
	}

	// TODO: close failures channel when framework is stopped.
	for ft := range failures {
		etcdutil.ReportFailure(f.etcdClient, f.name, ft)
	}
}
