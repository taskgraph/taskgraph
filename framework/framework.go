package framework

import (
	"fmt"
	"io"
	"log"
	"encoding/json"
	"math"
	"net"
	"hash/fnv"

	"github.com/coreos/go-etcd/etcd"
	"../../taskgraph"
	"github.com/taskgraph/taskgraph/filesystem"
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
	mapreduceConfig
}

type mapreduceConfig struct {
	mapperNum uint64
	shuffleNum uint64
	reducerNum uint64
	azureClient *filesystem.AzureClient
	outputContainerName string
	outputBlobName string
	shuffleWriteCloser []io.WriteCloser
	outputWriter io.WriteCloser
	mapperFunc func (taskgraph.Framework, string)
	reducerFunc func (taskgraph.Framework, string, []string)
}

type emitKV struct {
	Key string `json:"key"`
	Value string `json:"value"`
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


// TODO :
// may have synchronization issues 
// promote to buffer stream 
func (f *framework) Emit(key string, val string) {
	h := fnv.New32a()
	h.Write([]byte(key))
	var KV emitKV
	KV.Key = key
	KV.Value = val
	toShuffle := h.Sum32() % uint32(f.shuffleNum)
	
	data, err := json.Marshal(KV)
	data = append(data, '\n')
	if err != nil {
		f.log.Fatalf("json marshal error : ", err)
	}
	f.shuffleWriteCloser[toShuffle].Write(data)
}

func (f *framework) Collect(key string, val string) {
	f.outputWriter.Write([]byte(key + " " + val + "\n"));
}

func (f *framework) GetMapperNum() uint64 { return f.mapperNum }

func (f *framework) GetShuffleNum() uint64 { return f.shuffleNum }

func (f *framework) GetReducerNum() uint64 { return f.reducerNum }

func (f *framework) GetAzureClient() *filesystem.AzureClient { return f.azureClient}

func (f *framework) GetMapperFunc() func(taskgraph.Framework, string) { return f.mapperFunc }

func (f *framework) GetReducerFunc() func(taskgraph.Framework, string, []string) { return f.reducerFunc }

func (f *framework) GetOutputContainerName() string { return f.outputContainerName }

func (f *framework) GetLogger() *log.Logger { return f.log }

func (f *framework) GetTaskID() uint64 { return f.taskID }

func (f *framework) GetEpoch() uint64 { return f.epoch }
