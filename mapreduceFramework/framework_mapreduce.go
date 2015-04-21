package mapreduceFramework

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net"
	"strconv"
	"bufio"

	"../../taskgraph"
	"../filesystem"
	"../pkg/etcdutil"
	"github.com/coreos/go-etcd/etcd"
	"golang.org/x/net/context"
)

const exitEpoch = math.MaxUint64

type mapreducerFramework struct {
	// These should be passed by outside world
	name     string
	etcdURLs []string
	log      *log.Logger

	// user defined interfaces
	taskBuilder taskgraph.MapreduceTaskBuilder
	topology    taskgraph.Topology

	task          taskgraph.MapreduceTask
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
	mapperNum          uint64
	shuffleNum         uint64
	reducerNum         uint64
	client             filesystem.Client
	outputDirName      string
	outputFileName     string
	shuffleWriteCloser []*bufio.Writer
	outputWriter       *bufio.Writer
	mapperFunc         func(taskgraph.MapreduceFramework, string)
	reducerFunc        func(taskgraph.MapreduceFramework, string, []string)
	writerBufferSize int
	readerBufferSize int
}

// set default buffer size
const defaultWriteBufferSize = 4096

type emitKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// The key type is unexported to prevent collisions with context keys defined in
// other packages.
type contextKey int

// epochkey is the context key for the epoch.  Its value of zero is
// arbitrary.  If this package defined other context keys, they would have
// different integer values.
const epochKey contextKey = 1

func (f *mapreducerFramework) FlagMeta(ctx context.Context, linkType, meta string) {
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

// When app code invoke this method on framework, we simply // update the etcd epoch to next uint64. All nodes should watch
// for epoch and update their local epoch correspondingly.
func (f *mapreducerFramework) IncEpoch(ctx context.Context) {
	epoch, ok := ctx.Value(epochKey).(uint64)
	if !ok {
		f.log.Fatalf("Can not find epochKey in IncEpoch")
	}
	f.log.Println(f.name, epoch, epoch+1)
	err := etcdutil.CASEpoch(f.etcdClient, f.name, epoch, epoch+1)
	f.log.Println(f.name, epoch, epoch+1)
	if err != nil {
		f.log.Fatalf("task %d Epoch CompareAndSwap(%d, %d) failed: %v",
			f.taskID, epoch+1, epoch, err)
	}
}

func (f *mapreducerFramework) GetTopology() taskgraph.Topology { return f.topology }

func (f *mapreducerFramework) Kill() {
	// framework select loop will quit and end like getting a exit epoch, except that
	// it won't set exit epoch across cluster.
	f.log.Fatalf("t ask %d finish work, kill it", f.taskID)
	close(f.epochWatcher)
}

// When node call this on framework, it simply set epoch to exitEpoch,
// All nodes will be notified of the epoch change and exit themselves.
func (f *mapreducerFramework) ShutdownJob() {
	if err := etcdutil.CASEpoch(f.etcdClient, f.name, f.epoch, exitEpoch); err != nil {
		panic("TODO: we should do a set instead of CAS here.")
	}
	if err := etcdutil.SetJobStatus(f.etcdClient, f.name, 0); err != nil {
		panic("SetJobStatus")
	}
}

// When user call this on framework, it transfer key/value data to specific shuffle
// Generally, only mapper function emit data
func (f *mapreducerFramework) Emit(key string, val string) {
	if f.shuffleNum == 0 {
		return
	}
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

// Clean
// remove file in filesystem
func (f *mapreducerFramework) Clean(path string) {	
	err := f.client.Remove(path)
	if err != nil {
		f.log.Fatal(err)
	}		
}

// When mapper task begin, it invoke this to create shuffleNum buffer writer
func (f *mapreducerFramework) SetMapperOutputWriter() {
	var toShuffle uint64
	for toShuffle = 0; toShuffle < f.shuffleNum; toShuffle++ {
		shufflePath := f.outputDirName + "/" + strconv.FormatUint(f.taskID, 10) + "mapper" + strconv.FormatUint(toShuffle, 10)
		f.Clean(shufflePath)
		shuffleWriteCloserNow, err := f.client.OpenWriteCloser(shufflePath)
		if err != nil {
			f.log.Fatalf("Create filesystem client writeCloser failed, error : %v", err)
		}
		f.shuffleWriteCloser = append(f.shuffleWriteCloser, bufio.NewWriterSize(shuffleWriteCloserNow, defaultWriteBufferSize))
	}
}

// When reducer task begin, 
// it invoke this to create a buffer writer to output its result
func (f *mapreducerFramework) SetReducerOutputWriter() {
	reducerPath := f.outputDirName + "/" + "reducerResult" + strconv.FormatUint(f.taskID, 10)
	f.Clean(reducerPath)
	outputWriter, err := f.client.OpenWriteCloser(reducerPath)
	if err != nil {
		f.log.Fatalf("Create filesystem client writeCloser failed, error : %v", err)
		return
	}
	f.outputWriter = bufio.NewWriterSize(outputWriter, f.writerBufferSize)
}

func (f *mapreducerFramework) SetWriterBufferSize(bufferSize int) {
	f.writerBufferSize = bufferSize
} 

func (f *mapreducerFramework) SetReaderBufferSize(bufferSize int) {
	f.readerBufferSize = bufferSize
}

func (f *mapreducerFramework) Collect(key string, val string) {
	f.outputWriter.Write([]byte(key + " " + val + "\n"))
}

// When mapper finished, it need invoke this to flush buffer
// to output remaining data in buffer to shuffle
func (f *mapreducerFramework) FinishMapper() {
	var toShuffle uint64
	for toShuffle = 0; toShuffle < f.shuffleNum; toShuffle++ {
		f.shuffleWriteCloser[toShuffle].Flush()
	}
}

// When reducer finished, it need invoke this to flush buffer
// to output remaining data in buffer to final task
func (f *mapreducerFramework) FinishReducer() {
	f.outputWriter.Flush()
}

func (f *mapreducerFramework) GetMapperNum() uint64 { return f.mapperNum }

func (f *mapreducerFramework) GetShuffleNum() uint64 { return f.shuffleNum }

func (f *mapreducerFramework) GetReducerNum() uint64 { return f.reducerNum }

func (f *mapreducerFramework) GetClient() filesystem.Client { return f.client }

func (f *mapreducerFramework) GetMapperFunc() func(taskgraph.MapreduceFramework, string) {
	return f.mapperFunc
}

func (f *mapreducerFramework) GetReducerFunc() func(taskgraph.MapreduceFramework, string, []string) {
	return f.reducerFunc
}

func (f *mapreducerFramework) GetReaderBufferSize() int { return f.readerBufferSize }

func (f *mapreducerFramework) GetWriterBufferSize() int { return f.writerBufferSize }

func (f *mapreducerFramework) GetOutputDirName() string { return f.outputDirName }

func (f *mapreducerFramework) GetOutputFileName() string { return f.outputFileName }

func (f *mapreducerFramework) GetLogger() *log.Logger { return f.log }

func (f *mapreducerFramework) GetTaskID() uint64 { return f.taskID }

func (f *mapreducerFramework) GetEpoch() uint64 { return f.epoch }
