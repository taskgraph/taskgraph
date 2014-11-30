package framework

import (
	"log"
	"net"
	"os"

	"github.com/go-distributed/meritop"
	"github.com/go-distributed/meritop/pkg/etcdutil"
)

// One need to pass in at least these two for framework to start. The config
// is used to pass on to task implementation for its configuration.
func NewBootStrap(jobName string, etcdURLs []string, config meritop.Config, ln net.Listener, logger *log.Logger) meritop.Bootstrap {
	return &framework{
		name:     jobName,
		etcdURLs: etcdURLs,
		config:   config,
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

	// First, we fetch the current global epoch from etcd.
	f.epoch, err = f.fetchEpoch()
	if err != nil {
		f.log.Fatal("Can not parse epoch from etcd")
	}

	if f.taskID, err = f.occupyTask(); err != nil {
		f.log.Fatalf("occupyTask failed: %v", err)
	}

	// task builder and topology are defined by applications.
	// Both should be initialized at this point.
	// Get the task implementation and topology for this node (indentified by taskID)
	f.task = f.taskBuilder.GetTask(f.taskID)
	f.topology.SetTaskID(f.taskID)

	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	f.etcdClient.Create(etcdutil.MakeParentMetaPath(f.name, f.GetTaskID()), "", 0)
	f.etcdClient.Create(etcdutil.MakeChildMetaPath(f.name, f.GetTaskID()), "", 0)
	f.watchAll(roleParent, f.topology.GetParents(f.epoch))
	f.watchAll(roleChild, f.topology.GetChildren(f.epoch))

	// We need to first watch epoch.
	f.watchEpoch()

	go f.startHTTP()
	f.dataRespChan = make(chan *dataResponse, 100)
	go f.dataResponseReceiver()

	// After framework init finished, it should init task.
	f.task.Init(f.taskID, f, f.config)

	for f.epoch != maxUint64 {
		f.task.SetEpoch(f.epoch)
		select {
		case f.epoch = <-f.epochChan:
			// TODO: cleanup resources.
		case <-f.epochStop:
			return
		}
	}

	// clean up resources
	f.stop()
}
