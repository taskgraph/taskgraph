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

	if f.taskID, err = f.occupyTask(); err != nil {
		// if err := f.standby(); err != nil {
		// 	f.log.Fatalf("occupyTask failed: %v", err)
		// }
		f.log.Fatal("doesn't support standby now")
	}

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)
	// task init is put before any other routines.
	// For example, if a watch of parent meta is triggered but task isn't init-ed
	// yet, then there will a null pointer access
	f.task.Init(f.taskID, f)

	f.epochChan = make(chan uint64, 1)
	f.epochStop = make(chan bool, 1)
	// meta will have epoch prepended so we must get epoch before any watch on meta
	f.epoch, err = etcdutil.GetAndWatchEpoch(f.etcdClient, f.name, f.epochChan, f.epochStop)
	if err != nil {
		f.log.Fatalf("WatchEpoch failed: %v", err)
	}
	f.log.Printf("task %d starting at epoch %d\n", f.taskID, f.epoch)

	f.heartbeat()
	// go f.detectAndReportFailures()

	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	f.watchAll(roleParent, f.topology.GetParents(f.epoch))
	f.watchAll(roleChild, f.topology.GetChildren(f.epoch))

	f.dataRespChan = make(chan *frameworkhttp.DataResponse, 100)
	f.dataReqStop = make(chan struct{})
	go f.startHTTP()
	go f.dataResponseReceiver()

	defer f.releaseResource()
	for f.epoch != exitEpoch {
		f.task.SetEpoch(f.epoch)
		var ok bool
		f.epoch, ok = <-f.epochChan
		if !ok {
			break
		}
	}
}

// Framework http server for data request.
// Each request will be in the format: "/datareq?taskID=XXX&req=XXX".
// "taskID" indicates the requesting task. "req" is the meta data for this request.
// On success, it should respond with requested data in http body.
func (f *framework) startHTTP() {
	f.log.Printf("task %d serving http on %s\n", f.taskID, f.ln.Addr())
	// TODO: http server graceful shutdown
	epocher := frameworkhttp.Epocher(f)
	handler := frameworkhttp.NewDataRequestHandler(f.topology, f.task, epocher)
	if err := http.Serve(f.ln, handler); err != nil {
		f.log.Fatalf("http.Serve() returns error: %v\n", err)
	}
}

// occupyTask will grab the first unassigned task and register itself on etcd.
func (f *framework) occupyTask() (uint64, error) {
	// get all nodes under task dir
	slots, err := f.etcdClient.Get(etcdutil.MakeTaskDirPath(f.name), true, true)
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
		var taskCallback func(uint64, string)
		switch who {
		case roleParent:
			// Watch parent's child.
			watchPath = etcdutil.MakeChildMetaPath(f.name, taskID)
			taskCallback = f.task.ParentMetaReady
		case roleChild:
			// Watch child's parent.
			watchPath = etcdutil.MakeParentMetaPath(f.name, taskID)
			taskCallback = f.task.ChildMetaReady
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
					f.log.Fatalf("WARN: not a unit64 prepended to meta: %s", values[0])
				}
				if ep < f.epoch {
					continue
				}
				taskCallback(taskID, values[1])
			}
		}(receiver, taskID)
	}
	f.stops = append(f.stops, stops...)
}
