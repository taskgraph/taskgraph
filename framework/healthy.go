package framework

import (
	"math"
	"time"

	"github.com/go-distributed/meritop/pkg/etcdutil"
)

var (
	heartbeatInterval = 15 * time.Second
)

// TODO: we need to let framework pass in stop chan
func (f *framework) heartbeat() {
	err := etcdutil.Heartbeat(f.etcdClient, f.name, f.taskID, heartbeatInterval, make(chan struct{}))
	if err != nil {
		f.log.Printf("Heartbeat stops with failure: %v\n", err)
	}
}

func (f *framework) detectAndReportFailures() {
	failures := make(chan uint64)
	// TODO: We assume all tasks are setup at this point. So if there is task
	// not having `health` nodes, it implies node failure.
	time.Sleep(1000 * time.Millisecond)
	// assume the topo does not change with epoch for now
	// TODO: stop routines...
	for _, id := range f.topology.GetChildren(f.epoch) {
		go func(id uint64) {
			for {
				failures <- etcdutil.DetectFailure(f.etcdClient, f.name, id, make(chan bool))
			}
		}(id)
	}
	for _, id := range f.topology.GetParents(f.epoch) {
		go func(id uint64) {
			for {
				failures <- etcdutil.DetectFailure(f.etcdClient, f.name, id, make(chan bool))
			}
		}(id)
	}

	// TODO: close failures channel when framework is stopped.
	for ft := range failures {
		if ft != math.MaxUint64 {
			etcdutil.ReportFailure(f.etcdClient, f.name, ft)
		}
	}
}
