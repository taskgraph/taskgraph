package regression

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"github.com/taskgraph/taskgraph/pkg/common"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*
The dummy task is designed for regresion test of taskgraph framework.
This works with tree topology.
The main idea behind the regression test is following:
There will be two kinds of dummyTasks: master and slaves. We will have one master
sits at the top with taskID = 0, and then rest 6 (2^n - 2) tasks forms a tree under
the master. There will be 10 epochs, from 1 to 10, at each epoch, we send out a
vector with all values equal to epochID, and each slave is supposedly return a vector
with all values equals epochID*taskID, the values are reduced back to master, and
master will print out the epochID and aggregated vector. After all 10 epoch, it kills
job.
*/

// dummyData is used to carry parameter and gradient;
type dummyData struct {
	Value int32
}

// dummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type dummyMaster struct {
	dataChan           chan int32
	NodeProducer       chan bool
	framework          taskgraph.Framework
	epoch, taskID      uint64
	logger             *log.Logger
	config             map[string]string
	numberOfIterations uint64

	param        *pb.Parameter
	gradient     *pb.Gradient
	fromChildren map[uint64]*pb.Gradient
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummyMaster) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
func (t *dummyMaster) Exit() {}

// Ideally, we should also have the following:
func (t *dummyMaster) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	if linkType == "Children" {
		t.logger.Printf("master ChildMetaReady, task: %d, epoch: %d, child: %d\n", t.taskID, t.epoch, fromID)
		// Get data from child. When all the data is back, starts the next epoch.
		switch meta {
		case "GradientReady":
			t.framework.DataRequest(ctx, fromID, "/proto.Regression/GetGradient", &pb.Input{t.epoch})
		default:
			panic("")
		}
	}
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) SetEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("master SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	if t.testablyFail("SetEpoch", strconv.FormatUint(epoch, 10)) {
		return
	}

	t.param = new(pb.Parameter)
	t.gradient = new(pb.Gradient)

	t.epoch = epoch
	t.param.Value = int32(t.epoch)

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*pb.Gradient)
	t.framework.FlagMeta(ctx, "Parents", "ParamReady")
}

// These are payload rpc for application purpose.
func (t *dummyMaster) GetParameter(ctx context.Context, input *pb.Input) (*pb.Parameter, error) {
	err := t.framework.CheckEpoch(input.Epoch)
	if err != nil {
		return nil, err
	}
	return t.param, nil
}

func (t *dummyMaster) GetGradient(ctx context.Context, input *pb.Input) (*pb.Gradient, error) {
	panic("")
}

func (t *dummyMaster) ChildDataReady(ctx context.Context, childID uint64, output proto.Message) {
	d, ok := output.(*pb.Gradient)
	if !ok {
		panic("")
	}
	t.fromChildren[childID] = d

	t.logger.Printf("master ChildDataReady, task: %d, epoch: %d, child: %d, ready: %d\n",
		t.taskID, t.epoch, childID, len(t.fromChildren))

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetNeighbors("Children", t.epoch)) {
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}

		t.dataChan <- t.gradient.Value
		// TODO(xiaoyunwu) We need to do some test here.

		// In real ML, we modify the gradient first. But here it is noop.
		if t.epoch == t.numberOfIterations {
			if t.config["writefile"] != "" {
				data := []byte(fmt.Sprintf("Finished job. Gradient value: %v\n", t.gradient.Value))
				ioutil.WriteFile(t.config["writefile"], data, 0644)
			}
			t.framework.ShutdownJob()
		} else {
			t.logger.Printf("master finished current epoch, task: %d, epoch: %d", t.taskID, t.epoch)
			t.framework.IncEpoch(ctx)
		}
	}
}

func (t *dummyMaster) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	if method == "/proto.Regression/GetGradient" {
		t.ChildDataReady(ctx, fromID, output)
		return
	}
	panic("")
}

func (t *dummyMaster) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.Regression/GetParameter":
		return new(pb.Parameter)
	case "/proto.Regression/GetGradient":
		return new(pb.Gradient)
	default:
		t.logger.Panicf("Unknown method: %s", methodName)
		return nil
	}
}

func (t *dummyMaster) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterRegressionServer(server, t)
	return server
}

func (t *dummyMaster) testablyFail(method string, args ...string) bool {
	if t.config == nil {
		return false
	}
	if t.config[method] != "fail" {
		return false
	}
	if len(args) >= 1 && t.config["failepoch"] != "" {
		// we need to care about fail at specific epoch
		if t.config["failepoch"] != args[0] {
			return false
		}
	}
	if !probablyFail(t.config["faillevel"]) {
		return false
	}
	t.logger.Printf("master task %d testably fail, method: %s\n", t.taskID, method)
	t.framework.Kill()
	t.NodeProducer <- true
	return true
}

// dummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type dummySlave struct {
	framework     taskgraph.Framework
	epoch, taskID uint64
	logger        *log.Logger
	NodeProducer  chan bool
	config        map[string]string

	param          *pb.Parameter
	gradient       *pb.Gradient
	fromChildren   map[uint64]*pb.Gradient
	gradientReady  *common.CountdownLatch
	parameterReady *common.CountdownLatch
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummySlave) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
func (t *dummySlave) Exit() {}

// Ideally, we should also have the following
func (t *dummySlave) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	if linkType == "Parents" {
		t.logger.Printf("slave ParentMetaReady, task: %d, epoch: %d\n", t.taskID, t.epoch)
		t.framework.DataRequest(ctx, fromID, "/proto.Regression/GetParameter", &pb.Input{t.epoch})
	}
	if linkType == "Children" {
		t.logger.Printf("slave ChildMetaReady, task: %d, epoch: %d\n", t.taskID, t.epoch)
		go func() {
			// If a new node restart and find out both parent and child meta ready, it will
			// simultaneously request both data. We need to wait until gradient data is there.
			t.gradientReady.Await()
			t.framework.DataRequest(ctx, fromID, "/proto.Regression/GetGradient", &pb.Input{t.epoch})
		}()
	}
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) SetEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)

	t.param = new(pb.Parameter)
	t.gradient = new(pb.Gradient)
	t.gradientReady = common.NewCountdownLatch(1)
	t.parameterReady = common.NewCountdownLatch(1)

	t.epoch = epoch
	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]*pb.Gradient)
}

func (t *dummySlave) GetParameter(ctx context.Context, input *pb.Input) (*pb.Parameter, error) {
	err := t.framework.CheckEpoch(input.Epoch)
	if err != nil {
		return nil, err
	}
	// There is a race:
	//   A -> B -> C (parent -> child)
	//   B has flagged "parameter Ready"
	//   Now B crashed, and C crashed. C restarted and found B has flagged "parameter Ready".
	//   C requested B. B needs to await until it actually has the data.
	t.parameterReady.Await()
	return t.param, nil
}

func (t *dummySlave) GetGradient(ctx context.Context, input *pb.Input) (*pb.Gradient, error) {
	err := t.framework.CheckEpoch(input.Epoch)
	if err != nil {
		return nil, err
	}
	return t.gradient, nil
}

func (t *dummySlave) ParentDataReady(ctx context.Context, parentID uint64, output proto.Message) {
	t.logger.Printf("slave ParentDataReady, task: %d, epoch: %d, parent: %d\n", t.taskID, t.epoch, parentID)
	if t.testablyFail("ParentDataReady") {
		return
	}
	if t.gradientReady.Count() == 0 {
		return
	}
	d, ok := output.(*pb.Parameter)
	if !ok {
		panic("")
	}
	t.param = d
	t.parameterReady.CountDown()
	// We need to carry out local compuation.
	t.gradient.Value = t.param.Value * int32(t.framework.GetTaskID())
	t.gradientReady.CountDown()

	// If this task has children, flag meta so that children can start pull
	// parameter.
	children := t.framework.GetTopology().GetNeighbors("Children", t.epoch)
	if len(children) != 0 {
		t.framework.FlagMeta(ctx, "Parents", "ParamReady")
	} else {
		// On leaf node, we can immediately return by and flag parent
		// that this node is ready.
		t.framework.FlagMeta(ctx, "Children", "GradientReady")
	}
}

func (t *dummySlave) ChildDataReady(ctx context.Context, childID uint64, output proto.Message) {
	d, ok := output.(*pb.Gradient)
	if !ok {
		panic("")
	}
	t.fromChildren[childID] = d

	t.logger.Printf("slave ChildDataReady, task: %d, epoch: %d, child: %d, ready: %d\n",
		t.taskID, t.epoch, childID, len(t.fromChildren))

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetNeighbors("Children", t.epoch)) {
		// In real ML, we add the gradient first.
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}

		// If this failure happens, a new node will redo computing again.
		if t.testablyFail("ChildDataReady") {
			return
		}

		t.framework.FlagMeta(ctx, "Children", "GradientReady")

		// if this failure happens, the parent could
		// 1. not have the data yet. In such case, the parent could
		//   1.1 not request the data before a new node restarts. This will cause
		//       double requests since we provide at-least-once semantics (!outdated).
		//   1.2 request the data with a failed host (request should fail or be
		//       responded with error message).
		// 2. already get the data.
		if t.testablyFail("ChildDataReady") {
			return
		}
	}
}

func (t *dummySlave) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	if method == "/proto.Regression/GetParameter" {
		t.ParentDataReady(ctx, fromID, output)
	} else {
		t.ChildDataReady(ctx, fromID, output)
	}
}

func (t *dummySlave) testablyFail(method string, args ...string) bool {
	if t.config == nil {
		return false
	}
	if t.config[method] != "fail" {
		return false
	}
	if !probablyFail(t.config["faillevel"]) {
		return false
	}
	t.logger.Printf("slave task %d testably fail, method: %s\n", t.taskID, method)
	t.framework.Kill()
	t.NodeProducer <- true
	return true
}
func (t *dummySlave) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.Regression/GetParameter":
		return new(pb.Parameter)
	case "/proto.Regression/GetGradient":
		return new(pb.Gradient)
	default:
		t.logger.Panicf("Unknown method: %s", methodName)
		return nil
	}
}

func (t *dummySlave) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterRegressionServer(server, t)
	return server
}

func probablyFail(levelStr string) bool {
	level, err := strconv.Atoi(levelStr)
	if err != nil {
		return false
	}
	if level < rand.Intn(100)+1 {
		return false
	}
	return true
}

// used for testing
type SimpleTaskBuilder struct {
	GDataChan          chan int32
	NumberOfIterations uint64
	NodeProducer       chan bool
	MasterConfig       map[string]string
	SlaveConfig        map[string]string
}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc SimpleTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	if taskID == 0 {
		return &dummyMaster{
			dataChan:           tc.GDataChan,
			NodeProducer:       tc.NodeProducer,
			config:             tc.MasterConfig,
			numberOfIterations: tc.NumberOfIterations,
		}
	}
	return &dummySlave{
		NodeProducer: tc.NodeProducer,
		config:       tc.SlaveConfig,
	}
}
