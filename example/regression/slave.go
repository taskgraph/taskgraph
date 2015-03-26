package regression

import (
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"github.com/taskgraph/taskgraph/pkg/common"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

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
func (t *dummySlave) EnterEpoch(ctx context.Context, epoch uint64) {
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
