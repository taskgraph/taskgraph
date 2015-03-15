package framework

import (
	"fmt"
	"log"
	"math"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/framework/frameworkhttp"
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

	task       taskgraph.Task
	taskID     uint64
	epoch      uint64
	etcdClient *etcd.Client
	ln         net.Listener

	// A meta is a signal for specific epoch some task has some data.
	// However, our fault tolerance mechanism will start another task if it failed
	// and flag the same meta again. Therefore, we keep track of  notified meta.
	metaNotified map[string]bool

	// etcd stops
	metaStops []chan bool
	epochStop chan bool

	httpStop      chan struct{}
	heartbeatStop chan struct{}

	epochPassed chan struct{}

	// event loop
	epochChan          chan uint64
	metaChan           chan *metaChange
	dataReqtoSendChan  chan *dataRequest
	dataReqChan        chan *dataRequest
	dataRespToSendChan chan *dataResponse
	dataRespChan       chan *frameworkhttp.DataResponse
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type contextKey int

// epochkey is the context key for the epoch.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const epochKey contextKey = 1

// Now use google context, for we simply create a barebone and attach the epoch to it.
func (f *framework) createContext() context.Context {
	return context.WithValue(context.Background(), epochKey, f.epoch)
}

func (f *framework) FlagMeta(ctxt context.Context, linkType, meta string) {
	epoch, ok := ctxt.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in FlagMeta: %d", epoch)
	}
	value := fmt.Sprintf("%d-%s", epoch, meta)
	_, err := f.etcdClient.Set(etcdutil.MetaPath(linkType, f.name, f.GetTaskID()), value, 0)
	if err != nil {
		f.log.Fatalf("etcdClient.Set failed; key: %s, value: %s, error: %v",
			etcdutil.MetaPath(linkType, f.name, f.GetTaskID()), value, err)
	}
}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *framework) IncEpoch(ctxt context.Context) {
	epoch, ok := ctxt.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in IncEpoch")
	}
	err := etcdutil.CASEpoch(f.etcdClient, f.name, epoch, epoch+1)
	if err != nil {
		f.log.Fatalf("task %d Epoch CompareAndSwap(%d, %d) failed: %v",
			f.taskID, epoch+1, epoch, err)
	}
}

func (f *framework) DataRequest(ctxt context.Context, toID uint64, req string) {
	epoch, ok := ctxt.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in DataRequest")
	}

	// assumption here:
	// Event driven task will call this in a synchronous way so that
	// the epoch won't change at the time task sending this request.
	// Epoch may change, however, before the request is actually being sent.
	f.dataReqtoSendChan <- &dataRequest{
		taskID: toID,
		epoch:  epoch,
		req:    req,
	}
}

func (f *framework) GetTopology() taskgraph.Topology { return f.topology }

// this will shutdown local node instead of global job.
func (f *framework) stop() {
	close(f.epochChan)
}

func (f *framework) Kill() {
	f.stop()
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
