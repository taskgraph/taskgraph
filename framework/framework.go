package framework

import (
	"fmt"
	"log"
	"math"
	"net"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/framework/frameworkhttp"
	"github.com/go-distributed/meritop/pkg/etcdutil"
	"github.com/go-distributed/meritop/pkg/topoutil"
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

	task          meritop.Task
	taskID        uint64
	epoch         uint64
	epochChan     chan uint64
	epochStop     chan bool // for etcd
	heartbeatStop chan struct{}
	etcdClient    *etcd.Client
	stops         []chan bool // for etcd
	ln            net.Listener
	dataRespChan  chan *frameworkhttp.DataResponse
	dataReqStop   chan struct{}
}

type dataResponse struct {
	taskID uint64
	req    string
	data   []byte
}

// Framework event loop handles data response for requests sent in DataRequest().
func (f *framework) dataResponseReceiver() {
	for dataResp := range f.dataRespChan {
		switch {
		case topoutil.IsParent(f.topology, f.epoch, dataResp.TaskID):
			go f.task.ParentDataReady(dataResp.TaskID, dataResp.Req, dataResp.Data)
		case topoutil.IsChild(f.topology, f.epoch, dataResp.TaskID):
			go f.task.ChildDataReady(dataResp.TaskID, dataResp.Req, dataResp.Data)
		default:
			panic("unimplemented")
		}
	}
}

func (f *framework) FlagMetaToParent(meta string) {
	value := fmt.Sprintf("%d-%s", f.epoch, meta)
	_, err := f.etcdClient.Set(etcdutil.MakeParentMetaPath(f.name, f.GetTaskID()), value, 0)
	if err != nil {
		f.log.Fatalf("etcdClient.Set failed; key: %s, value: %s, error: %v",
			etcdutil.MakeParentMetaPath(f.name, f.GetTaskID()), value, err)
	}
}

func (f *framework) FlagMetaToChild(meta string) {
	value := fmt.Sprintf("%d-%s", f.epoch, meta)
	_, err := f.etcdClient.Set(etcdutil.MakeChildMetaPath(f.name, f.GetTaskID()), value, 0)
	if err != nil {
		f.log.Fatalf("etcdClient.Set failed; key: %s, value: %s, error: %v",
			etcdutil.MakeParentMetaPath(f.name, f.GetTaskID()), value, err)
	}
}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *framework) IncEpoch() {
	err := etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, f.epoch+1)
	if err != nil {
		f.log.Fatalf("Epoch CompareAndSwap(%d, %d) failed: %v", f.epoch+1, f.epoch, err)
	}
}

// getAddress will return the host:port address of the service taking care of
// the task that we want to talk to.
// Currently we grab the information from etcd every time. Local cache could be used.
// If it failed, e.g. network failure, it should return error.
func (f *framework) getAddress(id uint64) (string, error) {
	resp, err := f.etcdClient.Get(etcdutil.MakeTaskMasterPath(f.name, id), false, false)
	if err != nil {
		return "", err
	}
	return resp.Node.Value, nil
}

func (f *framework) DataRequest(toID uint64, req string) {
	addr, err := f.getAddress(toID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Fatalf("getAddress(%d) failed: %v", toID, err)
		return
	}
	go func() {
		d := frameworkhttp.RequestData(addr, f.taskID, toID, req)
		select {
		case f.dataRespChan <- d:
		case <-f.dataReqStop:
		}
	}()
}

func (f *framework) GetTopology() meritop.Topology { return f.topology }

func (f *framework) releaseResource() {
	f.log.Printf("task %d stops running. Releasing resources...\n", f.taskID)
	f.epochStop <- true
	close(f.heartbeatStop)
	close(f.dataReqStop) // must be closed before dataRespChan
	close(f.dataRespChan)
	for _, c := range f.stops {
		c <- true
	}
}

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
