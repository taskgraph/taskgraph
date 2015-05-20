package framework

import (
	"log"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/topo"

	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"github.com/taskgraph/taskgraph/pkg/etcdutil"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// TestRequestDataEpochMismatch creates a scenario where data request happened
// with two different epochs. In this case, the server should back pressure and
// request client should get notified and return error.
func TestRequestDataEpochMismatch(t *testing.T) {
	t.Skip("TODO")
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
	addr = addr
	// _, err = frameworkhttp.RequestData(addr, "Parents", "req", 0, fw.GetTaskID(), 10, fw.GetLogger())
	// if err != frameworkhttp.ErrReqEpochMismatch {
	// 	t.Fatalf("error want = %v, but get = (%)", frameworkhttp.ErrReqEpochMismatch, err.Error())
	// }
}

// TestFrameworkFlagMetaReady and TestFrameworkDataRequest test basic workflows of
// framework impl. It uses a scenario with two nodes: 0 as parent, 1 as child.
// The basic idea is that when parent tries to talk to child and vice versa,
// there will be some data transferring and captured by application task.
// Here we have implemented a helper user task to capture those data, test if
// it's passed from framework correctly and unmodified.
func TestFrameworkFlagMeta(t *testing.T) {
	appName := "TestFrameworkFlagMeta"
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

	ctx := context.WithValue(context.Background(), epochKey, uint64(0))
	for i, tt := range tests {
		// 0: F#FlagChildMetaReady -> 1: T#ParentMetaReady
		f0.FlagMeta(ctx, "Children", tt.cMeta)
		// from child(1)'s view
		data := <-cDataChan
		expected := &tDataBundle{id: 0, meta: tt.cMeta}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}

		// 1: F#FlagParentMetaReady -> 0: T#ChildMetaReady
		f1.FlagMeta(ctx, "Parents", tt.pMeta)
		// from parent(0)'s view
		data = <-pDataChan
		expected = &tDataBundle{id: 1, meta: tt.pMeta}
		if !reflect.DeepEqual(data, expected) {
			t.Errorf("#%d: data bundle want = %v, get = %v", i, expected, data)
		}
	}
}

func TestFrameworkDataRequest(t *testing.T) {
	appName := "framework_test_datarequest"
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
	ctx := context.WithValue(context.Background(), epochKey, uint64(0))

	f0.DataRequest(ctx, 1, "/proto.Regression/GetGradient", nil)
	data := <-pDataChan
	expected := &tDataBundle{
		id:     1,
		method: "/proto.Regression/GetGradient",
		output: &pb.Gradient{1},
	}
	if !reflect.DeepEqual(data, expected) {
		t.Errorf("data bundle want = %v, get = %v", expected, data)
	}
	f1.DataRequest(ctx, 0, "/proto.Regression/GetParameter", nil)
	data = <-cDataChan
	expected = &tDataBundle{
		id:     0,
		method: "/proto.Regression/GetParameter",
		output: &pb.Parameter{1},
	}
	if !reflect.DeepEqual(data, expected) {
		t.Errorf("data bundle want = %v, get = %v", expected, data)
	}
}

type tDataBundle struct {
	id     uint64
	meta   string
	method string
	output proto.Message
}

type testableTaskBuilder struct {
	cDataChan  chan *tDataBundle
	pDataChan  chan *tDataBundle
	setupLatch *sync.WaitGroup
}

func (b *testableTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	switch taskID {
	case 0:
		return &testableTask{dataChan: b.pDataChan,
			setupLatch: b.setupLatch}
	case 1:
		return &testableTask{dataChan: b.cDataChan,
			setupLatch: b.setupLatch}
	default:
		panic("unimplemented")
	}
}

type testableTask struct {
	id         uint64
	framework  taskgraph.Framework
	setupLatch *sync.WaitGroup

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
func (t *testableTask) EnterEpoch(ctx context.Context, epoch uint64) {}

func (t *testableTask) MetaReady(ctx context.Context, fromID uint64, meta string) {
	if t.dataChan != nil {
		t.dataChan <- &tDataBundle{id: fromID, meta: meta}
	}
}

func (t *testableTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataChan <- &tDataBundle{id: fromID, method: method, output: output}
}

// These are payload rpc for application purpose.
func (t *testableTask) GetParameter(ctx context.Context, input *pb.Input) (*pb.Parameter, error) {
	return &pb.Parameter{1}, nil
}

func (t *testableTask) GetGradient(ctx context.Context, input *pb.Input) (*pb.Gradient, error) {
	return &pb.Gradient{1}, nil
}

func (t *testableTask) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.Regression/GetParameter":
		return new(pb.Parameter)
	case "/proto.Regression/GetGradient":
		return new(pb.Gradient)
	default:
		log.Fatalf("Unknown method: %s", methodName)
		return nil
	}
}

func (t *testableTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterRegressionServer(server, t)
	return server
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("create listener failed: %v", err)
	}
	return l
}
