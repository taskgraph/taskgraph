package framework

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/go-distributed/meritop"
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

	if f.taskID, err = f.occupyTask(); err != nil {
		f.log.Println("standbying...")
		if err := f.standby(); err != nil {
			f.log.Fatalf("standby failed: %v", err)
		}
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
	f.heartbeat()

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)

	// TODO(hongchao): I haven't figured out a way to shut server down..
	go f.startHTTP()

	f.eventloop()
	f.releaseResource()
}

func (f *framework) eventloop() {
	f.metaChan = make(chan *metaChange, 100)
	f.dataReqtoSendChan = make(chan *dataRequest, 100)
	f.dataReqRecvedChan = make(chan *dataRequest, 100)
	f.dataRespChan = make(chan *dataResponse, 100)

	// from this point the task will start doing work
	f.task.Init(f.taskID, f)
	f.setEpochStarted()

	f.log.Printf("task %d starts event loop", f.taskID)
	defer f.log.Printf("task %d exits event loop!", f.taskID)
	for {
		select {
		case nextEpoch, ok := <-f.epochChan:
			f.releaseEpochResource()
			if !ok { // single task exit
				nextEpoch = exitEpoch
				return
			}
			if f.epoch = nextEpoch; f.epoch == exitEpoch {
				return
			}
			// start the next epoch's work
			f.setEpochStarted()
		case mc := <-f.metaChan:
			if mc.epoch != f.epoch {
				break
			}
			switch mc.who {
			case roleParent:
				go f.task.ParentMetaReady(mc.from, mc.meta)
			case roleChild:
				go f.task.ChildMetaReady(mc.from, mc.meta)
			default:
				panic("unimplemented")
			}
		case req := <-f.dataReqtoSendChan:
			if req.Epoch != f.epoch {
				break
			}
			go f.sendRequest(req)
		case req := <-f.dataReqRecvedChan:
			if req.Epoch != f.epoch {
				// TODO: we need to back pressure that epoch is different
				panic("unimplemented")
			}
			go f.handleDataReq(req)
			// case <-datarespsend:
		case resp := <-f.dataRespChan:
			if resp.Epoch != f.epoch {
				break
			}
			go f.handleDataResp(resp)
		}

		if f.epoch == exitEpoch {
			break
		}
	}
}

func (f *framework) setEpochStarted() {
	f.task.SetEpoch(f.epoch)

	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	f.watchAll(roleParent, f.topology.GetParents(f.epoch))
	f.watchAll(roleChild, f.topology.GetChildren(f.epoch))
}

func (f *framework) releaseEpochResource() {
	for _, c := range f.stops {
		c <- true
	}
	f.stops = nil
}

// release resources: heartbeat, epoch watch.
func (f *framework) releaseResource() {
	f.log.Printf("task %d stops running. Releasing resources...\n", f.taskID)
	f.epochStop <- true
	close(f.heartbeatStop)
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("task %d serving http on %s\n", f.taskID, f.ln.Addr())
	// TODO: http server graceful shutdown
	handler := &dataReqHandler{f.dataReqRecvedChan}
	if err := http.Serve(f.ln, handler); err != nil {
		f.log.Fatalf("http.Serve() returns error: %v\n", err)
	}
}

// occupyTask will grab the first unassigned task and register itself on etcd.
func (f *framework) occupyTask() (uint64, error) {
	// get all nodes under task dir
	slots, err := f.etcdClient.Get(etcdutil.TaskDirPath(f.name), true, true)
	if err != nil {
		return 0, err
	}
	for _, s := range slots.Node.Nodes {
		idstr := path.Base(s.Key)
		id, err := strconv.ParseUint(idstr, 0, 64)
		if err != nil {
			f.log.Fatalf("WARN: taskID isn't integer, registration on etcd has been corrupted!")
			continue
		}
		ok := etcdutil.TryOccupyTask(f.etcdClient, f.name, id, f.ln.Addr().String())
		if ok {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no unassigned task found")
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
			panic("unimplemented")
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
					panic(fmt.Sprintf("WARN: not a unit64 prepended to meta: %s", values[0]))
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
	f.stops = append(f.stops, stops...)
}
