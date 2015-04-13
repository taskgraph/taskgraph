package mapreduce

import (
	"fmt"
	"log"
	"math"
	"io"
	"net"
	"hash/fnv"
	"json"

	"github.com/taskgraph/filesystem"
	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"

)

const exitEpoch = math.MaxUint64

type mpFramework struct {
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
	metaStops      []chan bool
	epochWatchStop chan bool

	globalStop chan struct{}

	// event loop
	epochWatcher      chan uint64
	metaChan          chan *metaChange
	dataReqtoSendChan chan *dataRequest
	dataRespChan      chan *dataResponse
	epochCheckChan    chan *epochCheck

	// mapreduce config
	mapperNum uint64
	shuffleNum uint64
	reducerNum uint64
	azureClient filesystem.AzureClient
	containerName string
	shuffleWriteCloser []io.WriteCloser
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type contextKey int

// epochkey is the context key for the epoch.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const epochKey contextKey = 1

func (f *mpFramework) InitWithMapreduceConfig(mpNum uint64, sfNum uint64, rdNum uint64, azureAccountName string, azureAccountKey string, containerName string) {
	f.mapperNum = mpNum
	f.shuffleNum = sfNum
	f.reducerNum = rdNum
	f.azureClient, err = filesystem.NewAzureClient(
		azureAccountKey, 
		azureAccountName,  
		"core.chinacloudapi.cn", 
        "2014-02-14", 
        true
    )
    f.shuffleWriteCloser = make(io.WriteCloser(), f.shuffleNum) 
    f.containerName = containerName
    for i := 0; i < f.shuffleNum; i++ {
		shuffleblobName := f.containerName + "/shuffle" + i;
		shuffleWriteCloser[i], err := f.azureClient.openWriteCloser(shuffleblobName)
		if err != nil {
			f.log.Fatalf(err)
		}
    }
    
	if err != nil {
		f.log.Fatalf("Create azure stroage client failed, error : %v", err)
	}


}

func (f *mpFramework) FlagMeta(ctx context.Context, linkType, meta string) {
	epoch, ok := ctx.Value(epochKey).(uint64)
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

// TODO :
// may have corruent
func (f *mpFramework) Emit(string key, string val) {
	h := fnv.New32a()
	h.Write([]byte(key))
	toShuffle := h.Sum32() % f.shuffleNum
	
	data := []byte(key + " " + val + "\n")
	shuffleWriteCloser[toShuffle].write(data)

}

// When app code invoke this method on framework, we simply
// update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *mpFramework) IncEpoch(ctx context.Context) {
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

func (f *mpFramework) GetTopology() taskgraph.Topology { return f.topology }

func (f *mpFramework) Kill() {
	// framework select loop will quit and end like getting a exit epoch, except that
	// it won't set exit epoch across cluster.
	close(f.epochWatcher)
}

// When node call this on framework, it simply set epoch to exitEpoch,
// All nodes will be notified of the epoch change and exit themselves.
func (f *mpFramework) ShutdownJob() {
	if err := etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, exitEpoch); err != nil {
		panic("TODO: we should do a set instead of CAS here.")
	}
	if err := etcdutil.SetJobStatus(f.etcdClient, f.name, 0); err != nil {
		panic("SetJobStatus")
	}
}

func (f *mpFramework) GetLogger() *log.Logger { return f.log }

func (f *mpFramework) GetTaskID() uint64 { return f.taskID }

func (f *mpFramework) GetEpoch() uint64 { return f.epoch }
