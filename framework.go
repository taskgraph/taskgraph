package meritop

import (
	"log"

	"github.com/coreos/go-etcd/etcd"
)

// This interface is used by application during taskgraph configuration phase.
type Bootstrap interface {
	// These allow application developer to set the task configuration so framework
	// implementation knows which task to invoke at each node.
	SetTaskBuilder(taskBuilder TaskBuilder)

	// This allow the application to specify how tasks are connection at each epoch
	SetTopology(topology Topology)

	// After all the configure is done, driver need to call start so that all
	// nodes will get into the event loop to run the application.
	Start()
}

// Note that framework can decide how update can be done, and how to serve the updatelog.
type BackedUpFramework interface {
	// Ask framework to do update on this update on this task, which consists
	// of one primary and some backup copies.
	Update(taskID uint64, log UpdateLog)
}

type Framework interface {
	// These two are useful for task to inform the framework their status change.
	// metaData has to be really small, since it might be stored in etcd.
	// Flags and Sends the metaData to parent of the current task.
	FlagParentMetaReady(meta Metadata)
	FlagChildMetaReady(meta Metadata)

	// This allow the task implementation query its neighbors.
	GetTopology() Topology

	// Some task can inform all participating tasks to exit.
	Exit()

	// Some task can inform all participating tasks to new epoch
	SetEpoch(epoch uint64)

	GetLogger() log.Logger

	// Request data from parent or children.
	DataRequest(toID uint64, meta Metadata)

	// This allow task implementation to node corresponding to taskID so that
	// it can carry out application dependent communication.
	GetNode(taskID uint64) Node

	// This is used to figure out taskid for current node
	GetTaskID() uint64
}

type framework struct {
	// These should be passed by outside world
	name     string
	etcdURLs []string

	// user defined interfaces
	task     Task
	topology Topology

	taskID     uint64
	epoch      uint64
	etcdClient *etcd.Client
	stops      []chan bool
}

func (f *framework) start() {
	f.etcdClient = etcd.NewClient(f.etcdURLs)
	f.topology.SetTaskID(f.taskID)
	f.epoch = 0
	f.stops = make([]chan bool, 0)

	// setup etcd watches
	// - create self's parent and child meta flag
	// - watch parents' child meta flag
	// - watch children's parent meta flag
	f.etcdClient.Create(
		MakeTaskParentMetaPath(f.name, f.GetTaskID()),
		"", 0)
	f.etcdClient.Create(
		MakeTaskChildMetaPath(f.name, f.GetTaskID()),
		"", 0)
	parentStops := f.watchAll(
		f.topology.GetParents(f.epoch),
		MakeTaskChildMetaPath,
		f.task.ParentMetaReady)
	childStops := f.watchAll(
		f.topology.GetChildren(f.epoch),
		MakeTaskParentMetaPath,
		f.task.ChildMetaReady)

	f.stops = append(f.stops, parentStops...)
	f.stops = append(f.stops, childStops...)

	// After framework init finished, it should init task.
	f.task.SetEpoch(f.epoch)
	f.task.Init(f.taskID, f, nil)
}

func (f *framework) stop() {
	for _, c := range f.stops {
		close(c)
	}
}

func (f *framework) FlagParentMetaReady(meta Metadata) {
	f.etcdClient.Set(
		MakeTaskParentMetaPath(f.name, f.GetTaskID()),
		"",
		0)
}

func (f *framework) FlagChildMetaReady(meta Metadata) {
	f.etcdClient.Set(
		MakeTaskChildMetaPath(f.name, f.GetTaskID()),
		"",
		0)
}

func (f *framework) SetEpoch(epoch uint64) {
	f.epoch = epoch
}

func (f *framework) watchAll(taskIDs []uint64,
	makeTaskMetaPath func(string, uint64) string,
	taskCallback func(uint64, Metadata)) []chan bool {
	stops := make([]chan bool, len(taskIDs))

	for i, taskID := range taskIDs {
		receiver := make(chan *etcd.Response, 10)
		stop := make(chan bool, 1)
		stops[i] = stop

		go f.etcdClient.Watch(
			makeTaskMetaPath(f.name, taskID),
			0,
			false,
			receiver,
			stop)

		go func(receiver <-chan *etcd.Response, taskID uint64) {
			for {
				resp, ok := <-receiver
				if !ok {
					return
				}
				if resp.Action != "set" {
					continue
				}
				taskCallback(taskID, nil)
			}
		}(receiver, taskID)
	}
	return stops
}

func (f *framework) DataRequest(toID uint64, meta Metadata) {
}

func (f *framework) GetTopology() Topology {
	panic("unimplemented")
}

func (f *framework) Exit() {
}

func (f *framework) GetLogger() log.Logger {
	panic("unimplemented")
}

func (f *framework) GetNode(taskID uint64) Node {
	panic("unimplemented")
}

func (f *framework) GetTaskID() uint64 {
	return f.taskID
}
