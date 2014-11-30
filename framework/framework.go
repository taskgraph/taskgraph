package framework

import (
	"log"
	"net"

	"strconv"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/framework/frameworkhttp"
	"github.com/go-distributed/meritop/pkg/etcdutil"
	"github.com/go-distributed/meritop/pkg/topoutil"
)

// This is used as special value to indicate that it is the last epoch, time
// to exit.
const maxUint64 uint64 = ^uint64(0)

type framework struct {
	// These should be passed by outside world
	name     string
	etcdURLs []string
	config   meritop.Config
	log      *log.Logger

	// user defined interfaces
	taskBuilder meritop.TaskBuilder
	topology    meritop.Topology

	task         meritop.Task
	taskID       uint64
	epoch        uint64
	epochChan    <-chan uint64
	epochStop    chan bool
	etcdClient   *etcd.Client
	stops        []chan bool
	ln           net.Listener
	dataRespChan chan *frameworkhttp.DataResponse
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

func (f *framework) stop() {
	close(f.dataRespChan)
	f.epochStop <- true
	for _, c := range f.stops {
		c <- true
	}
}

func (f *framework) FlagMetaToParent(meta string) {
	f.etcdClient.Set(etcdutil.MakeParentMetaPath(f.name, f.GetTaskID()), meta, 0)
}

func (f *framework) FlagMetaToChild(meta string) {
	f.etcdClient.Set(etcdutil.MakeChildMetaPath(f.name, f.GetTaskID()), meta, 0)
}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *framework) IncEpoch() {
	_, err := f.etcdClient.CompareAndSwap(
		etcdutil.MakeJobEpochPath(f.name),
		strconv.FormatUint(f.epoch+1, 10),
		0, strconv.FormatUint(f.epoch, 10), 0)
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
	// getAddressFromTaskID
	addr, err := f.getAddress(toID)
	if err != nil {
		// TODO: We should handle network faults later by retrying
		f.log.Fatalf("getAddress(%d) failed: %v", toID, err)
		return
	}
	go func() {
		f.dataRespChan <- frameworkhttp.RequestData(addr, f.taskID, toID, req)
	}()
}

func (f *framework) GetTopology() meritop.Topology { return f.topology }

// When node call this on framework, it simply set epoch to a maxUint64,
// All nodes will be notified of the epoch change and exit themselves.
func (f *framework) ShutdownJob() {
	maxUint64Str := strconv.FormatUint(maxUint64, 10)
	f.etcdClient.Set(etcdutil.MakeJobEpochPath(f.name), maxUint64Str, 0)
}

func (f *framework) GetLogger() *log.Logger { return f.log }

func (f *framework) GetTaskID() uint64 { return f.taskID }

func (f *framework) GetEpoch() uint64 { return f.epoch }
