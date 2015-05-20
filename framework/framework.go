package framework

import (
	"fmt"
	"log"
	"math"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
)

const exitEpoch = math.MaxUint64

type framework struct {
	// These should be passed by outside world
	name     string
	etcdURLs []string
	log      *log.Logger

	// user defined interfaces
	taskBuilder taskgraph.TaskBuilder
	topology    taskgraph.Topology

	task          taskgraph.Task
	taskID        uint64
	epoch         uint64
	etcdClient    *etcd.Client
	ln            net.Listener
	userCtx       context.Context
	userCtxCancel context.CancelFunc

	// A meta is a signal for specific epoch some task has some data.
	// However, our fault tolerance mechanism will start another task if it failed
	// and flag the same meta again. Therefore, we keep track of  notified meta.
	metaNotified map[string]bool

	// etcd stops
	metaWatchStop  chan bool
	epochWatchStop chan bool

	globalStop chan struct{}

	// event loop
	epochWatcher      chan uint64
	metaChan          chan *metaChange
	dataReqtoSendChan chan *dataRequest
	dataRespChan      chan *dataResponse
	epochCheckChan    chan *epochCheck
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type contextKey int

// epochkey is the context key for the epoch.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const epochKey contextKey = 1

func (f *framework) FlagMeta(ctx context.Context, linkType, meta string) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Panicf("Can not find epochKey in FlagMeta, epoch: %d", epoch)
	}
	// send the meta change notification to every task of specified link type.
	for _, id := range f.topology.GetNeighbors(linkType, epoch) {
		// The value is made of "epoch-fromID-metadata"
		//
		// Epoch is prepended to meta. When a new one starts and replaces
		// the old one, it doesn't need to handle previous things, whose
		// epoch is smaller than current one.
		value := fmt.Sprintf("%d-%d-%s", epoch, f.taskID, meta)
		_, err := f.etcdClient.Set(etcdutil.MetaPath(f.name, id), value, 0)
		if err != nil {
			f.log.Panicf("etcdClient.Set failed; key: %s, value: %s, error: %v",
				etcdutil.MetaPath(f.name, id), value, err)
		}
	}
}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *framework) IncEpoch(ctx context.Context) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in IncEpoch")
	}
	err := etcdutil.CASEpoch(f.etcdClient, f.name, epoch, epoch+1)
	if err != nil {
		f.log.Fatalf("task %d Epoch CompareAndSwap(%d, %d) failed: %v",
			f.taskID, epoch+1, epoch, err)
	}
}

func (f *framework) GetTopology() taskgraph.Topology { return f.topology }

func (f *framework) Kill() {
	// framework select loop will quit and end like getting a exit epoch, except that
	// it won't set exit epoch across cluster.
	close(f.epochWatcher)
}

// When node call this on framework, it simply set epoch to exitEpoch,
// All nodes will be notified of the epoch change and exit themselves.
func (f *framework) ShutdownJob() {
	if err := etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, exitEpoch); err != nil {
		panic("TODO: we should do a set instead of CAS here.")
	}
	if err := etcdutil.SetJobStatus(f.etcdClient, f.name, 0); err != nil {
		panic("SetJobStatus")
	}
}

func (f *framework) GetLogger() *log.Logger { return f.log }

func (f *framework) GetTaskID() uint64 { return f.taskID }

func (f *framework) GetEpoch() uint64 { return f.epoch }
