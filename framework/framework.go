package framework

import (
	"fmt"
	"log"
	"math"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

const exitEpoch = math.MaxUint64

type framework struct {
	// These should be passed by outside world
	name     string
	etcdURLs []string
	log      *log.Logger

	// user defined interfaces
	taskBuilder meritop.TaskBuilder
	topology    meritop.Topology

	task       meritop.Task
	taskID     uint64
	epoch      uint64
	etcdClient *etcd.Client
	ln         net.Listener

	// etcd stops
	stops     []chan bool
	epochStop chan bool

	heartbeatStop chan struct{}

	// event loop
	epochChan          chan uint64
	metaChan           chan *metaChange
	dataReqtoSendChan  chan *dataRequest
	dataReqChan        chan *dataRequest
	dataRespToSendChan chan *dataRequest
	dataRespChan       chan *dataResponse
}

func (f *framework) FlagMetaToParent(meta string) {
	value := fmt.Sprintf("%d-%s", f.epoch, meta)
	_, err := f.etcdClient.Set(etcdutil.ParentMetaPath(f.name, f.GetTaskID()), value, 0)
	if err != nil {
		f.log.Fatalf("etcdClient.Set failed; key: %s, value: %s, error: %v",
			etcdutil.ParentMetaPath(f.name, f.GetTaskID()), value, err)
	}
}

func (f *framework) FlagMetaToChild(meta string) {
	value := fmt.Sprintf("%d-%s", f.epoch, meta)
	_, err := f.etcdClient.Set(etcdutil.ChildMetaPath(f.name, f.GetTaskID()), value, 0)
	if err != nil {
		f.log.Fatalf("etcdClient.Set failed; key: %s, value: %s, error: %v",
			etcdutil.ChildMetaPath(f.name, f.GetTaskID()), value, err)
	}
}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *framework) IncEpoch() {
	err := etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, f.epoch+1)
	if err != nil {
		f.log.Fatalf("task %d Epoch CompareAndSwap(%d, %d) failed: %v",
			f.taskID, f.epoch+1, f.epoch, err)
	}
}

// getAddress will return the host:port address of the service taking care of
// the task that we want to talk to.
// Currently we grab the information from etcd every time. Local cache could be used.
// If it failed, e.g. network failure, it should return error.
func (f *framework) getAddress(id uint64) (string, error) {
	resp, err := f.etcdClient.Get(etcdutil.TaskMasterPath(f.name, id), false, false)
	if err != nil {
		return "", err
	}
	return resp.Node.Value, nil
}

func (f *framework) DataRequest(toID uint64, req string) {
	// assumption here:
	// Event driven task will call this in a synchronous way so that
	// the epoch won't change at the time task sending this request.
	// Epoch may change, however, before the request is actually being sent.
	f.dataReqtoSendChan <- &dataRequest{
		TaskID: toID,
		Epoch:  f.epoch,
		Req:    req,
	}
}

func (f *framework) sendRequest(dr *dataRequest) {
	addr, err := f.getAddress(dr.TaskID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Fatalf("getAddress(%d) failed: %v", dr.TaskID, err)
		return
	}
	d := requestData(addr, dr.Req, f.taskID, dr.TaskID, dr.Epoch, f.log)
	f.dataRespChan <- d
}

func (f *framework) GetTopology() meritop.Topology { return f.topology }

// this will shutdown local node instead of global job.
func (f *framework) stop() {
	close(f.epochChan)
}

// When node call this on framework, it simply set epoch to exitEpoch,
// All nodes will be notified of the epoch change and exit themselves.
func (f *framework) ShutdownJob() {
	etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, exitEpoch)
}

func (f *framework) GetLogger() *log.Logger { return f.log }

func (f *framework) GetTaskID() uint64 { return f.taskID }

func (f *framework) GetEpoch() uint64 { return f.epoch }
