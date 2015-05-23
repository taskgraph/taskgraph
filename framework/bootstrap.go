package framework

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
)

// One need to pass in at least these two for framework to start.
func NewBootStrap(jobName string, etcdURLs []string, ln net.Listener, logger *log.Logger) taskgraph.Bootstrap {
	return &framework{
		name:     jobName,
		etcdURLs: etcdURLs,
		ln:       ln,
		log:      logger,
	}
}

func (f *framework) SetTaskBuilder(taskBuilder taskgraph.TaskBuilder) {
	f.taskBuilder = taskBuilder
}

//The user add their topology link to the original topology by this function
func (f *framework) AddLinkage(linkType string, topology taskgraph.Topology) {
	if f.topology == nil {
		f.topology = make(map[string]taskgraph.Topology)
	}
	f.topology[linkType] = topology
}

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

	// SetTaskID to every linkType topology in our map topology container
	for key, _ := range f.topology {
		f.topology[key].SetTaskID(f.taskID)
	}

	f.heartbeat()
	f.setup()
	f.task.Init(f.taskID, f)
	f.run()
	f.releaseResource()
	f.task.Exit()
}

func (f *framework) setup() {
	f.globalStop = make(chan struct{})
	f.metaChan = make(chan *metaChange, 1)
	f.dataReqtoSendChan = make(chan *dataRequest, 1)
	f.dataRespChan = make(chan *dataResponse, 1)
	f.epochCheckChan = make(chan *epochCheck, 1)
}

func (f *framework) run() {
	f.log.Printf("framework starts to run")
	defer f.log.Printf("framework stops running.")
	f.setEpochStarted()
	go f.startHTTP()
	// Central event handling
	for {
		select {
		case nextEpoch, ok := <-f.epochWatcher:
			f.releaseEpochResource()
			if !ok { // task is killed
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
			f.handleMetaChange(f.userCtx, meta.from, meta.linkType, meta.meta)
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

func (f *framework) setEpochStarted() {
	// Each epoch have a new meta map
	f.metaNotified = make(map[string]bool)

	f.userCtx = context.WithValue(context.Background(), epochKey, f.epoch)
	f.userCtx, f.userCtxCancel = context.WithCancel(f.userCtx)

	f.task.EnterEpoch(f.userCtx, f.epoch)

	// setup etcd watches
	f.watchMeta()
}

func (f *framework) releaseEpochResource() {
	f.userCtxCancel()
	f.metaWatchStop <- true
}

// release resources: heartbeat, epoch watch.
func (f *framework) releaseResource() {
	f.log.Printf("framework is releasing resources...\n")
	f.epochWatchStop <- true
	close(f.globalStop)
	f.ln.Close() // stop grpc server
}

// occupyTask will grab the first unassigned task and register itself on etcd.
func (f *framework) occupyTask() error {
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

func parseMetaValue(val string) (epoch, taskID uint64, linkType, meta string, err error) {
	values := strings.SplitN(val, "-", 4)
	// epoch is prepended to meta. When a new one starts and replaces
	// the old one, it doesn't need to handle previous things, whose
	// epoch is smaller than current one.
	epoch, err = strconv.ParseUint(values[0], 10, 64)
	if err != nil {
		err = fmt.Errorf("not an unit64 epoch prepended: %s", values[0])
		return
	}
	taskID, err = strconv.ParseUint(values[1], 10, 64)
	if err != nil {
		err = fmt.Errorf("not an unit64 taskID prepended: %s", values[1])
		return
	}
	linkType = values[2]
	meta = values[3]
	return
}

// watch meta flag and receive any notification.
func (f *framework) watchMeta() {
	watchPath := etcdutil.MetaPath(f.name, f.taskID)

	// The function parses the watch response of meta flag, and passes the result
	// to central event handling.
	responseHandler := func(resp *etcd.Response) {
		// Note: Why "get"?
		// We first try to get the index. If there's value in it, we also parse it.
		if resp.Action != "set" && resp.Action != "get" {
			return
		}
		epoch, taskID, linkType, meta, err := parseMetaValue(resp.Node.Value)
		if err != nil {
			f.log.Panicf("parseMetaValue failed: %v", err)
		}
		f.metaChan <- &metaChange{
			from:     taskID,
			epoch:    epoch,
			linkType: linkType,
			meta:     meta,
		}
	}

	f.metaWatchStop = make(chan bool, 1)
	// Need to pass in taskID to make it work. Didn't know why.
	err := etcdutil.WatchMeta(f.etcdClient, watchPath, f.metaWatchStop, responseHandler)
	if err != nil {
		f.log.Panicf("WatchMeta failed. path: %s, err: %v", watchPath, err)
	}
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
