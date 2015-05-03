package framework

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"../../taskgraph"
	"../mapreduce/pkg/etcdutil"
	"github.com/coreos/go-etcd/etcd"
	"golang.org/x/net/context"
)

const defaultBufferSize = 4096

type mapreducerFramework struct {
	framework
	config    taskgraph.MapreduceConfig
	epochTopo taskgraph.Topology
}

// mapreduceFramework extends from framework, thus it could not use anonymous construction
// All mapreduceFramework function is same as framework, only need to do is convert to framework namespace
func NewMapreduceBootStrap(jobName string, etcdURLs []string, ln net.Listener, logger *log.Logger) taskgraph.MapreduceBootstrap {
	mpFramework := mapreducerFramework{}
	mpFramework.name = jobName
	mpFramework.etcdURLs = etcdURLs
	mpFramework.ln = ln
	mpFramework.log = logger
	return &mpFramework
}

func (f *mapreducerFramework) SetTaskBuilder(taskBuilder taskgraph.TaskBuilder) {
	f.taskBuilder = taskBuilder
}

func (f *mapreducerFramework) SetTopology(topology taskgraph.Topology) { f.topology = topology }

// Initialize Mapreduce configuration.
// Set default value to specfic variable
func (f *mapreducerFramework) InitWithMapreduceConfig(config taskgraph.MapreduceConfig) {
	if config.InterDir == "" {
		config.InterDir = "MapreducerProcessTemporaryResult"
	}
	if config.ReaderBufferSize == 0 {
		config.ReaderBufferSize = defaultBufferSize
	}
	if config.WriterBufferSize == 0 {
		config.WriterBufferSize = defaultBufferSize
	}
	f.config = config
}

func (f *mapreducerFramework) Start() {
	var err error

	if f.log == nil {
		f.log = log.New(os.Stdout, "", log.Lshortfile|log.Ltime|log.Ldate)
	}

	f.etcdClient = etcd.NewClient(f.etcdURLs)

	if err = f.occupyTask(); err != nil {
		f.log.Panicf("occupyTask() failed: %v", err)
	}

	f.log.SetPrefix(fmt.Sprintf("task %d: ", f.taskID))

	f.epochWatcher = make(chan uint64, 1) // grab epoch from etcd
	f.epochWatchStop = make(chan bool, 1) // stop etcd watch
	// meta will have epoch prepended so we must get epoch before any watch on meta
	f.epoch, err = etcdutil.GetAndWatchEpoch(f.etcdClient, f.name, f.epochWatcher, f.epochWatchStop)
	if err != nil {
		f.log.Fatalf("WatchEpoch failed: %v", err)
	}
	if f.epoch == exitEpoch {
		f.log.Printf("found that job has finished\n")
		f.epochWatchStop <- true
		return
	}
	f.log.Printf("starting at epoch %d\n", f.epoch)

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)

	f.heartbeat()
	f.setup()
	f.task.Init(f.taskID, f)
	f.task.(taskgraph.MapreduceTask).MapreduceConfiguration(f.config)
	f.run()
	f.releaseResource()
	f.task.Exit()
}

func (f *mapreducerFramework) setup() {
	f.globalStop = make(chan struct{})
	f.metaChan = make(chan *metaChange, 1)
	f.dataReqtoSendChan = make(chan *dataRequest, 1)
	f.dataRespChan = make(chan *dataResponse, 1)
	f.epochCheckChan = make(chan *epochCheck, 1)
}

func (f *mapreducerFramework) run() {
	f.log.Printf("framework starts to run")
	defer f.log.Printf("framework stops running.")
	f.setEpochStarted()
	go f.startHTTP()
	// this for-select is primarily used to synchronize epoch specific events.
	for {
		select {
		case nextEpoch, ok := <-f.epochWatcher:
			f.releaseEpochResource()
			if !ok { // task is killed
				f.log.Fatalf("task %d is killed", f.taskID)
				return
			}

			f.epoch = nextEpoch
			f.log.Printf("join epoch %d", nextEpoch)
			if f.epoch == exitEpoch {
				return
			}
			// start the next epoch's work
			f.setEpochStarted()
		case meta := <-f.metaChan:
			if meta.epoch != f.epoch {
				break
			}
			// We need to create a context before handling next event. The context saves
			// the epoch that was meant for this event. This context will be passed
			// to user event handler functions and used to ask framework to do work later
			// with previous information.
			f.handleMetaChange(f.userCtx, meta.from, meta.who, meta.meta)
		case req := <-f.dataReqtoSendChan:
			if req.epoch != f.epoch {
				f.log.Printf("abort data request, to %d, epoch %d, method %s", req.taskID, req.epoch, req.method)
				break
			}
			go f.sendRequest(req)
		case resp := <-f.dataRespChan:
			if resp.epoch != f.epoch {
				f.log.Printf("abort data response, to %d, epoch: %d, method %d", resp.taskID, resp.epoch, resp.method)
				break
			}
			f.handleDataResp(f.userCtx, resp)
		case ec := <-f.epochCheckChan:
			if ec.epoch != f.epoch {
				ec.fail()
				break
			}
			ec.pass()
		}
	}
}

func (f *mapreducerFramework) setEpochStarted() {
	// Each epoch have a new meta map
	f.metaNotified = make(map[string]bool)

	f.userCtx = context.WithValue(context.Background(), epochKey, f.epoch)
	f.userCtx, f.userCtxCancel = context.WithCancel(f.userCtx)

	f.task.EnterEpoch(f.userCtx, f.epoch)
	// setup etcd watches
	for _, linkType := range f.topology.GetLinkTypes() {
		f.watchMeta(linkType, f.topology.GetNeighbors(linkType, f.epoch))
	}
}

func (f *mapreducerFramework) releaseEpochResource() {
	f.userCtxCancel()
	for _, c := range f.metaStops {
		c <- true
	}
	f.metaStops = nil
}

// release resources: heartbeat, epoch watch.
func (f *mapreducerFramework) releaseResource() {
	f.log.Printf("framework is releasing resources...\n")
	f.epochWatchStop <- true
	close(f.globalStop)
	f.ln.Close() // stop grpc server
}

// occupyTask will grab the first unassigned task and register itself on etcd.
func (f *mapreducerFramework) occupyTask() error {
	for {
		freeTask, err := etcdutil.WaitFreeTask(f.etcdClient, f.name, f.log)
		if err != nil {
			return err
		}
		f.log.Printf("standby grabbed free task %d", freeTask)
		ok, err := etcdutil.TryOccupyTask(f.etcdClient, f.name, freeTask, f.ln.Addr().String())
		if err != nil {
			return err
		}
		if ok {
			f.taskID = freeTask
			return nil
		}
		f.log.Printf("standby tried task %d failed. Wait free task again.", freeTask)
	}
}

func (f *mapreducerFramework) watchMeta(linkType string, taskIDs []uint64) {
	stops := make([]chan bool, len(taskIDs))
	f.log.Println(f.taskID, linkType, taskIDs)
	for i, taskID := range taskIDs {
		stop := make(chan bool, 1)
		stops[i] = stop

		watchPath := etcdutil.MetaPath(linkType, f.name, taskID)

		// When a node working for a task crashed, a new node will take over
		// the task and continue what's left. It assumes that progress is stalled
		// until the new node comes (i.e. epoch won't change).
		responseHandler := func(resp *etcd.Response, taskID uint64) {
			if resp.Action != "set" && resp.Action != "get" {
				return
			}
			// epoch is prepended to meta. When a new one starts and replaces
			// the old one, it doesn't need to handle previous things, whose
			// epoch is smaller than current one.
			values := strings.SplitN(resp.Node.Value, "-", 2)
			ep, err := strconv.ParseUint(values[0], 10, 64)
			if err != nil {
				f.log.Panicf("WARN: not a unit64 prepended to meta: %s", values[0])
			}
			f.metaChan <- &metaChange{
				from:  taskID,
				who:   linkType,
				epoch: ep,
				meta:  values[1],
			}
		}

		// Need to pass in taskID to make it work. Didn't know why.
		err := etcdutil.WatchMeta(f.etcdClient, taskID, watchPath, stop, responseHandler)
		if err != nil {
			f.log.Panicf("WatchMeta failed. path: %s, err: %v", watchPath, err)
		}
	}
	f.metaStops = append(f.metaStops, stops...)
}

func (f *mapreducerFramework) handleMetaChange(ctx context.Context, taskID uint64, linkType, meta string) {
	// check if meta is handled before.
	tm := taskMeta(taskID, meta)
	if _, ok := f.metaNotified[tm]; ok {
		return
	}
	f.metaNotified[tm] = true

	f.task.MetaReady(ctx, taskID, linkType, meta)
}
