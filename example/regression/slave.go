package regression

import (
	"encoding/json"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"github.com/taskgraph/taskgraph/pkg/common"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// dummySlave is an prototype for data shard in machine learning applications.
// It mainly does to things, pass on parameters to its children, and collect
// gradient back then add them together before make it available to its parent.
type dummySlave struct {
	gradientReady  *common.CountdownLatch
	parameterReady *common.CountdownLatch
	taskCommon
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) SetEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	t.param = new(pb.Parameter)
	t.gradient = new(pb.Gradient)
	t.gradientReady = common.NewCountdownLatch(1)
	t.parameterReady = common.NewCountdownLatch(1)
	t.epoch = epoch
	t.fromChildren = make(map[uint64]*pb.Gradient)
}

// These are payload rpc for application purpose.
func (t *dummySlave) ServeAsParent(fromID uint64, req string) ([]byte, error) {
	// There is a race:
	//   A -> B -> C (parent -> child)
	//   B has flagged "parameter Ready"
	//   Now B crashed, and C crashed. C restarted and found B has flagged "parameter Ready".
	//   C requested B. B needs to await until it actually has the data.
	t.parameterReady.Await()
	return json.Marshal(t.param)
}

func (t *dummySlave) ServeAsChild(fromID uint64, req string) ([]byte, error) {
	return json.Marshal(t.gradient)
}

func (t *dummySlave) CreateGRPCServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterRegressionServer(server, t)
	return server
}

// Ideally, we should also have the following
func (t *dummySlave) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	if linkType == "Parents" {
		t.logger.Printf("slave ParentMetaReady, task: %d, epoch: %d\n", t.taskID, t.epoch)
		outputC := make(chan proto.Message, 1)
		t.framework.Fetch(ctx, fromID, "/proto.Regression/GetParameter", &pb.Input{t.epoch}, outputC)
		go t.ParentDataReady(ctx, fromID, meta, outputC)
	}
	if linkType == "Children" {
		t.logger.Printf("slave ChildMetaReady, task: %d, epoch: %d\n", t.taskID, t.epoch)
		outputC := make(chan proto.Message, 1)
		t.framework.Fetch(ctx, fromID, "/proto.Regression/GetGradient", &pb.Input{t.epoch}, outputC)
		go t.ChildDataReady(ctx, fromID, meta, outputC)
	}
}

func (t *dummySlave) ParentDataReady(ctx context.Context, parentID uint64, req string, outputC <-chan proto.Message) {
	select {
	case msg, ok := <-outputC:
		if !ok {
			return
		}
		t.logger.Printf("slave ParentDataReady, task: %d, epoch: %d, parent: %d\n", t.taskID, t.epoch, parentID)
		if t.testablyFail("ParentDataReady") {
			return
		}
		d, ok := msg.(*pb.Parameter)
		if !ok {
			t.logger.Fatalf("Can't convert message to Parameter: %v", msg)
		}
		t.param = d
		t.parameterReady.CountDown()
		// local compuation.
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
}

func (t *dummySlave) ChildDataReady(ctx context.Context, childID uint64, req string, outputC <-chan proto.Message) {
	select {
	case msg, ok := <-outputC:
		if !ok {
			return
		}
		d, ok := msg.(*pb.Gradient)
		if !ok {
			t.logger.Fatalf("Can't convert message to Gradient: %v", msg)
		}
		t.fromChildren[childID] = d
		t.logger.Printf("slave ChildDataReady, task: %d, epoch: %d, child: %d, ready: %d\n",
			t.taskID, t.epoch, childID, len(t.fromChildren))

		// We get all gradients from children. We should go into the next epoch now.
		if len(t.fromChildren) == len(t.framework.GetTopology().GetNeighbors("Children", t.epoch)) {
			// If a new node restart and find out both parent and child meta ready, it will
			// simultaneously request both data. We need to wait until gradient data is there.
			t.gradientReady.Await()

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
			// 1. not have the data yet. In such case, the parent would retry data request.
			// 2. already have the data.
			if t.testablyFail("ChildDataReady") {
				return
			}
		}
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
