package framework

import (
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework/frameworkhttp"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
)

// TestRequestDataEpochMismatch creates a scenario where data request happened
// with two different epochs. In this case, the server should back pressure and
// request client should get notified and return error.
func TestRequestDataEpochMismatch(t *testing.T) {
	job := "TestRequestDataEpochMismatch"
	etcdURLs := []string{"http://localhost:4001"}
	ctl := controller.New(job, etcd.NewClient(etcdURLs), 1, []string{"Parents", "Children"})
	ctl.InitEtcdLayout()
	defer ctl.DestroyEtcdLayout()

	fw := &framework{
		name:     job,
		etcdURLs: etcdURLs,
		ln:       createListener(t),
	}
	var wg sync.WaitGroup
	fw.SetTaskBuilder(&testableTaskBuilder{
		setupLatch: &wg,
	})
	fw.SetTopology(topo.NewTreeTopology(1, 1))
	wg.Add(1)
	go fw.Start()
	wg.Wait()
	defer fw.ShutdownJob()

	addr, err := etcdutil.GetAddress(fw.etcdClient, job, fw.GetTaskID())
	if err != nil {
		t.Fatalf("GetAddress failed: %v", err)
	}
	_, err = frameworkhttp.RequestData(addr, "req", 0, fw.GetTaskID(), 10, fw.GetLogger())
	if err != frameworkhttp.ErrReqEpochMismatch {
		t.Fatalf("error want = %v, but get = (%)", frameworkhttp.ErrReqEpochMismatch, err.Error())
	}
}

// TestFrameworkFlagMetaReady and TestFrameworkDataRequest test basic workflows of
// framework impl. It uses a scenario with two nodes: 0 as parent, 1 as child.
// The basic idea is that when parent tries to talk to child and vice versa,
// there will be some data transferring and captured by application task.
// Here we have implemented a helper user task to capture those data, test if
// it's passed from framework correctly and unmodified.
func TestFrameworkFlagMetaReady(t *testing.T) {
	appName := "framework_test_flagmetaready"
	etcdURLs := []string{"http://localhost:4001"}
	// launch controller to setup etcd layout
	ctl := controller.New(appName, etcd.NewClient(etcdURLs), 2, []string{"Parents", "Children"})
	if err := ctl.InitEtcdLayout(); err != nil {
		t.Fatalf("initEtcdLayout failed: %v", err)
	}
	defer ctl.DestroyEtcdLayout()

	pDataChan := make(chan *tDataBundle, 1)
	cDataChan := make(chan *tDataBundle, 1)

	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     appName,
		etcdURLs: etcdURLs,
		ln:       createListener(t),
	}
	f1 := &framework{
		name:     appName,
		etcdURLs: etcdURLs,
		ln:       createListener(t),
	}

	var wg sync.WaitGroup
	taskBuilder := &testableTaskBuilder{
		dataMap:    nil,
		cDataChan:  cDataChan,
		pDataChan:  pDataChan,
		setupLatch: &wg,
	}
	f0.SetTaskBuilder(taskBuilder)
	f0.SetTopology(topo.NewTreeTopology(2, 2))
	f1.SetTaskBuilder(taskBuilder)
	f1.SetTopology(topo.NewTreeTopology(2, 2))

	taskBuilder.setupLatch.Add(2)
	go f0.Start()
	go f1.Start()
	taskBuilder.setupLatch.Wait()
	if f0.GetTaskID() != 0 {
		f0, f1 = f1, f0
	}

	defer f0.ShutdownJob()

	tests := []struct {
		cMeta string
		pMeta string
	}{
		{"parent", "child"},
		{"ParamReady", "GradientReady"},
	}

	for i, tt := range tests {
		// 0: F#FlagChildMetaReady -> 1: T#ParentMetaReady
		f0.flagMetaToChild(tt.cMeta, 0)
		// from child(1)'s view
		data := <-pDataChan
		expected := &tDataBundle{0, tt.cMeta, "", nil}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}

		// 1: F#FlagParentMetaReady -> 0: T#ChildMetaReady
		f1.flagMetaToParent(tt.pMeta, 0)
		// from parent(0)'s view
		data = <-cDataChan
		expected = &tDataBundle{1, tt.pMeta, "", nil}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}
	}
}

func TestFrameworkDataRequest(t *testing.T) {
	appName := "framework_test_flagmetaready"
	etcdURLs := []string{"http://localhost:4001"}
	// launch controller to setup etcd layout
	ctl := controller.New(appName, etcd.NewClient(etcdURLs), 2, []string{"Parents", "Children"})
	if err := ctl.InitEtcdLayout(); err != nil {
		t.Fatalf("initEtcdLayout failed: %v", err)
	}
	defer ctl.DestroyEtcdLayout()

	tests := []struct {
		req  string
		resp []byte
	}{
		{"request", []byte("response")},
		{"parameters", []byte{1, 2, 3}},
		{"gradient", []byte{4, 5, 6}},
	}

	dataMap := make(map[string][]byte)
	for _, tt := range tests {
		dataMap[tt.req] = tt.resp
	}

	pDataChan := make(chan *tDataBundle, 1)
	cDataChan := make(chan *tDataBundle, 1)
	// simulate two tasks on two nodes -- 0 and 1
	// 0 is parent, 1 is child
	f0 := &framework{
		name:     appName,
		etcdURLs: etcdURLs,
		ln:       createListener(t),
	}
	f1 := &framework{
		name:     appName,
		etcdURLs: etcdURLs,
		ln:       createListener(t),
	}

	var wg sync.WaitGroup
	taskBuilder := &testableTaskBuilder{
		dataMap:    dataMap,
		cDataChan:  cDataChan,
		pDataChan:  pDataChan,
		setupLatch: &wg,
	}
	f0.SetTaskBuilder(taskBuilder)
	f0.SetTopology(topo.NewTreeTopology(2, 2))
	f1.SetTaskBuilder(taskBuilder)
	f1.SetTopology(topo.NewTreeTopology(2, 2))

	taskBuilder.setupLatch.Add(2)
	go f0.Start()
	go f1.Start()
	taskBuilder.setupLatch.Wait()
	if f0.GetTaskID() != 0 {
		f0, f1 = f1, f0
	}

	defer f0.ShutdownJob()

	for i, tt := range tests {
		// 0: F#DataRequest -> 1: T#ServeAsChild -> 0: T#ChildDataReady
		f0.dataRequest(1, tt.req, 0)
		// from child(1)'s view at 1: T#ServeAsChild
		data := <-pDataChan
		expected := &tDataBundle{0, "", data.req, nil}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}
		// from parent(0)'s view at 0: T#ChildDataReady
		data = <-cDataChan
		expected = &tDataBundle{1, "", data.req, data.resp}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}

		// 1: F#DataRequest -> 0: T#ServeAsParent -> 1: T#ParentDataReady
		f1.dataRequest(0, tt.req, 0)
		// from parent(0)'s view at 0: T#ServeAsParent
		data = <-cDataChan
		expected = &tDataBundle{1, "", data.req, nil}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}
		// from child(1)'s view at 1: T#ParentDataReady
		data = <-pDataChan
		expected = &tDataBundle{0, "", data.req, data.resp}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}
	}
}

type tDataBundle struct {
	id   uint64
	meta string
	req  string
	resp []byte
}

type testableTaskBuilder struct {
	dataMap    map[string][]byte
	cDataChan  chan *tDataBundle
	pDataChan  chan *tDataBundle
	setupLatch *sync.WaitGroup
}

func (b *testableTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	switch taskID {
	case 0:
		return &testableTask{dataMap: b.dataMap, dataChan: b.cDataChan,
			setupLatch: b.setupLatch}
	case 1:
		return &testableTask{dataMap: b.dataMap, dataChan: b.pDataChan,
			setupLatch: b.setupLatch}
	default:
		panic("unimplemented")
	}
}

type testableTask struct {
	id         uint64
	framework  taskgraph.Framework
	setupLatch *sync.WaitGroup
	// dataMap will be used to serve data according to request
	dataMap map[string][]byte

	// This channel is used to convey data passed from framework back to the main
	// thread, for checking. Thus it's initialized and passed in from outside.
	//
	// The basic idea is that there are only two nodes -- one parent and one child.
	// When this channel is for parent, it passes information from child.
	dataChan chan *tDataBundle
}

func (t *testableTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.id = taskID
	t.framework = framework
	if t.setupLatch != nil {
		t.setupLatch.Done()
	}
}
func (t *testableTask) Exit()                                        {}
func (t *testableTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {}

func (t *testableTask) MetaReady(ctx taskgraph.Context, fromID uint64, linkType, meta string) {
	if t.dataChan != nil {
		t.dataChan <- &tDataBundle{fromID, meta, "", nil}
	}
}

func (t *testableTask) ServeAsParent(fromID uint64, req string) ([]byte, error) {
	if t.dataChan != nil {
		t.dataChan <- &tDataBundle{fromID, "", req, nil}
	}
	return t.dataMap[req], nil
}

func (t *testableTask) ServeAsChild(fromID uint64, req string) ([]byte, error) {
	return t.ServeAsParent(fromID, req)
}

func (t *testableTask) ParentDataReady(ctx taskgraph.Context, fromID uint64, req string, resp []byte) {
	if t.dataChan != nil {
		t.dataChan <- &tDataBundle{fromID, "", req, resp}
	}
}

func (t *testableTask) ChildDataReady(ctx taskgraph.Context, fromID uint64, req string, resp []byte) {
	t.ParentDataReady(ctx, fromID, req, resp)
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}
