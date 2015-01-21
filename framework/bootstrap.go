package framework

import (
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/framework/frameworkhttp"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

type taskRole int

const (
	roleNone taskRole = iota
	roleParent
	roleChild
)

// One need to pass in at least these two for framework to start.
func NewBootStrap(jobName string, etcdURLs []string, ln net.Listener, logger *log.Logger) meritop.Bootstrap {
	return &framework{
		name:     jobName,
		etcdURLs: etcdURLs,
		ln:       ln,
		log:      logger,
	}
}

func (f *framework) SetTaskBuilder(taskBuilder meritop.TaskBuilder) { f.taskBuilder = taskBuilder }

func (f *framework) SetTopology(topology meritop.Topology) { f.topology = topology }

func (f *framework) Start() {
	var err error

	if f.log == nil {
		f.log = log.New(os.Stdout, "", log.Lshortfile|log.Ltime|log.Ldate)
	}

	f.etcdClient = etcd.NewClient(f.etcdURLs)

	if err = f.occupyTask(); err != nil {
		f.log.Fatalf("occupyTask() failed: %v", err)
	}

	f.epochChan = make(chan uint64, 1) // grab epoch from etcd
	f.epochStop = make(chan bool, 1)   // stop etcd watch
	// meta will have epoch prepended so we must get epoch before any watch on meta
	f.epoch, err = etcdutil.GetAndWatchEpoch(f.etcdClient, f.name, f.epochChan, f.epochStop)
	if err != nil {
		f.log.Fatalf("WatchEpoch failed: %v", err)
	}
	if f.epoch == exitEpoch {
		f.log.Printf("task %d found that job has finished\n", f.taskID)
		f.epochStop <- true
		return
	}
	f.log.Printf("task %d starting at epoch %d\n", f.taskID, f.epoch)

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)

	go f.startHTTP()

	f.heartbeat()
	f.setupChannels()
	f.task.Init(f.taskID, f)
	f.run()
	f.releaseResource()
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
	f.log.Printf("framework of task %d starts to run", f.taskID)
	defer f.log.Printf("framework of task %d stops running.", f.taskID)
	f.setEpochStarted()
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
			go f.handleMetaChange(f.createCtx(), meta.who, meta.from, meta.meta)
		case req := <-f.dataReqtoSendChan:
			if req.epoch != f.epoch {
				f.log.Printf("epoch mismatch: task %d, req-to-send epoch: %d, current epoch: %d",
					f.taskID, req.epoch, f.epoch)
				break
			}
			go f.sendRequest(req)
		case req := <-f.dataReqChan:
			if req.epoch != f.epoch {
				f.log.Printf("epoch mismatch: task %d, request epoch: %d, current epoch: %d",
					f.taskID, req.epoch, f.epoch)
				req.notifyEpochMismatch()
				break
			}
			go f.handleDataReq(req)
		case resp := <-f.dataRespToSendChan:
			if resp.epoch != f.epoch {
				f.log.Printf("epoch mismatch: task %d, resp-to-send epoch: %d, current epoch: %d",
					f.taskID, resp.epoch, f.epoch)
				resp.notifyEpochMismatch()
				break
			}
			go f.sendResponse(resp)
		case resp := <-f.dataRespChan:
			if resp.Epoch != f.epoch {
				f.log.Printf("epoch mismatch: task %d, response epoch: %d, current epoch: %d",
					f.taskID, resp.Epoch, f.epoch)
				break
			}
			go f.handleDataResp(f.createCtx(), resp)
		}
	}
}

func (f *framework) setEpochStarted() {
	f.task.SetEpoch(f.createCtx(), f.epoch)

	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	f.watchAll(roleParent, f.topology.GetParents(f.epoch))
	f.watchAll(roleChild, f.topology.GetChildren(f.epoch))
}

func (f *framework) releaseEpochResource() {
	for _, c := range f.metaStops {
		c <- true
	}
	f.metaStops = nil
}

// release resources: heartbeat, epoch watch.
func (f *framework) releaseResource() {
	f.log.Printf("framework of task %d is releasing resources...\n", f.taskID)
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
		f.log.Printf("standby got failure at task %d", freeTask)
		ok := etcdutil.TryOccupyTask(f.etcdClient, f.name, freeTask, f.ln.Addr().String())
		if ok {
			f.taskID = freeTask
			return nil
		}
		f.log.Printf("standby tried task %d failed. Wait free task again.", freeTask)
	}
}

func (f *framework) watchAll(who taskRole, taskIDs []uint64) {
	stops := make([]chan bool, len(taskIDs))

	for i, taskID := range taskIDs {
		receiver := make(chan *etcd.Response, 1)
		stop := make(chan bool, 1)
		stops[i] = stop

		var watchPath string
		switch who {
		case roleParent:
			// Watch parent's child-meta.
			watchPath = etcdutil.ChildMetaPath(f.name, taskID)
		case roleChild:
			// Watch child's parent-meta.
			watchPath = etcdutil.ParentMetaPath(f.name, taskID)
		default:
			f.log.Panic("unexpected")
		}

		// When a node working for a task crashed, a new node will take over
		// the task and continue what's left. It assumes that progress is stalled
		// until the new node comes (no middle stages being dismissed by newcomer).

		resp, err := f.etcdClient.Get(watchPath, false, false)
		var watchIndex uint64
		if err != nil {
			if !etcdutil.IsKeyNotFound(err) {
				f.log.Fatalf("etcd get failed (not KeyNotFound): %v", err)
			} else {
				watchIndex = 1
			}
		} else {
			watchIndex = resp.EtcdIndex + 1
			receiver <- resp
		}
		go f.etcdClient.Watch(watchPath, watchIndex, false, receiver, stop)
		go func(receiver <-chan *etcd.Response, taskID uint64) {
			for resp := range receiver {
				if resp.Action != "set" && resp.Action != "get" {
					continue
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
					who:   who,
					epoch: ep,
					meta:  values[1],
				}
			}
		}(receiver, taskID)
	}
	f.metaStops = append(f.metaStops, stops...)
}

func (f *framework) handleMetaChange(ctx meritop.Context, who taskRole, taskID uint64, meta string) {
	switch who {
	case roleParent:
		f.task.ParentMetaReady(ctx, taskID, meta)
	case roleChild:
		f.task.ChildMetaReady(ctx, taskID, meta)
	}
}
