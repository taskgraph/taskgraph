package framework

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/framework/frameworkhttp"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// One need to pass in at least these two for framework to start.
func NewBootStrap(jobName string, etcdURLs []string, ln net.Listener, gRPCServer *grpc.Server, logger *log.Logger) taskgraph.Bootstrap {
	return &framework{
		name:      jobName,
		etcdURLs:  etcdURLs,
		ln:        ln,
		log:       logger,
		rpcServer: gRPCServer,
	}
}

func (f *framework) SetTaskBuilder(taskBuilder taskgraph.TaskBuilder) { f.taskBuilder = taskBuilder }

func (f *framework) SetTopology(topology taskgraph.Topology) { f.topology = topology }

func (f *framework) Start() {
	var err error

	if f.log == nil {
		f.log = log.New(os.Stdout, "", log.Lshortfile|log.Ltime|log.Ldate)
	}

	f.etcdClient = etcd.NewClient(f.etcdURLs)

	if err = f.occupyTask(); err != nil {
		f.log.Panicf("occupyTask() failed: %v", err)
	}
	f.log.SetPrefix(fmt.Sprintf("task %d: ", f.taskID))

	f.epochChan = make(chan uint64, 1) // grab epoch from etcd
	f.epochStop = make(chan bool, 1)   // stop epoch watch
	// meta will have epoch prepended so we must get epoch before any watch on meta
	f.epoch, err = etcdutil.GetAndWatchEpoch(f.etcdClient, f.name, f.epochChan, f.epochStop)
	if err != nil {
		f.log.Fatalf("WatchEpoch failed: %v", err)
	}
	if f.epoch == exitEpoch {
		f.log.Printf("found that job has finished\n")
		f.epochStop <- true
		return
	}
	f.log.Printf("starting at epoch %d\n", f.epoch)

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)

	// TODO(grpc):
	// replace f.startHTTP() with:
	// go func(){
	//   	err:= grpcServer.Serve(listener)
	//   	... check err ...
	// }
	go f.startHTTP()

	f.heartbeat()
	f.setupChannels()
	f.task.Init(f.taskID, f)
	f.run()
	f.releaseResource()
	f.task.Exit()
}

func (f *framework) setupChannels() {
	f.httpStop = make(chan struct{})
	f.metaChan = make(chan *metaChange, 100)
	f.dataReqtoSendChan = make(chan *dataRequest, 100)
	f.dataReqChan = make(chan *dataRequest, 100)
	f.dataRespToSendChan = make(chan *dataResponse, 100)
	f.dataRespChan = make(chan *frameworkhttp.DataResponse, 100)
}

func (f *framework) run() {
	f.log.Printf("framework starts to run")
	defer f.log.Printf("framework stops running.")
	f.setEpochStarted()
	// this for-select is primarily used to synchronize epoch specific events.
	for {
		select {
		case nextEpoch, ok := <-f.epochChan:
			f.releaseEpochResource()
			if !ok { // single task exit
				nextEpoch = exitEpoch
				return
			}
			f.epoch = nextEpoch
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
			f.handleMetaChange(f.createContext(), meta.from, meta.who, meta.meta)
		case req := <-f.dataReqtoSendChan:
			if req.epoch != f.epoch {
				f.log.Printf("epoch mismatch: req-to-send epoch: %d, current epoch: %d", req.epoch, f.epoch)
				req.epochMismatch()
				break
			}
			req.send()
		case req := <-f.dataReqChan:
			if req.epoch != f.epoch {
				f.log.Printf("epoch mismatch: request epoch: %d, current epoch: %d", req.epoch, f.epoch)
				req.notifyEpochMismatch()
				break
			}
			f.handleDataReq(req)
		case resp := <-f.dataRespToSendChan:
			if resp.epoch != f.epoch {
				f.log.Printf("epoch mismatch: resp-to-send epoch: %d, current epoch: %d", resp.epoch, f.epoch)
				resp.notifyEpochMismatch()
				break
			}
			go f.sendResponse(resp)
		case resp := <-f.dataRespChan:
			if resp.Epoch != f.epoch {
				f.log.Printf("epoch mismatch: response epoch: %d, current epoch: %d", resp.Epoch, f.epoch)
				break
			}
			f.handleDataResp(f.createContext(), resp)
		}
	}
}

func (f *framework) setEpochStarted() {
	f.epochPassed = make(chan struct{})
	// Each epoch have a new meta map
	f.metaNotified = make(map[string]bool)
	f.requestCancels = make([]context.CancelFunc, 0)

	f.task.SetEpoch(f.createContext(), f.epoch)
	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	for _, linkType := range f.topology.GetLinkTypes() {
		f.watchMeta(linkType, f.topology.GetNeighbors(linkType, f.epoch))
	}
}

func (f *framework) releaseEpochResource() {
	close(f.epochPassed)
	for _, c := range f.metaStops {
		c <- true
	}
	f.metaStops = nil
	for _, cancel := range f.requestCancels {
		cancel()
	}
}

// release resources: heartbeat, epoch watch.
func (f *framework) releaseResource() {
	f.log.Printf("framework is releasing resources...\n")
	f.epochStop <- true
	close(f.heartbeatStop)
	f.stopHTTP()
}

// occupyTask will grab the first unassigned task and register itself on etcd.
func (f *framework) occupyTask() error {
	for {
		freeTask, err := etcdutil.WaitFreeTask(f.etcdClient, f.name, f.log)
		if err != nil {
			return err
		}
		f.log.Printf("standby grabbed free task %d", freeTask)
		ok := etcdutil.TryOccupyTask(f.etcdClient, f.name, freeTask, f.ln.Addr().String())
		if ok {
			f.taskID = freeTask
			return nil
		}
		f.log.Printf("standby tried task %d failed. Wait free task again.", freeTask)
	}
}

func (f *framework) watchMeta(linkType string, taskIDs []uint64) {
	stops := make([]chan bool, len(taskIDs))

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

func (f *framework) handleMetaChange(ctx context.Context, taskID uint64, linkType, meta string) {
	// check if meta is handled before.
	tm := taskMeta(taskID, meta)
	if _, ok := f.metaNotified[tm]; ok {
		return
	}
	f.metaNotified[tm] = true

	f.task.MetaReady(ctx, taskID, linkType, meta)

}

func taskMeta(taskID uint64, meta string) string {
	return fmt.Sprintf("%s-%s", strconv.FormatUint(taskID, 10), meta)
}
