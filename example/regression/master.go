package regression

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/golang/protobuf/proto"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// dummyMaster is prototype of parameter server, for now it does not
// carry out optimization yet. But it should be easy to add support when
// this full tests out.
// Note: in theory, since there should be no parent of this, so we should
// add error checing in the right places. We will skip these test for now.
type dummyMaster struct {
	dataChan           chan int32
	numberOfIterations uint64
	taskCommon
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) SetEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("master SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	if t.testablyFail("SetEpoch", strconv.FormatUint(epoch, 10)) {
		return
	}
	t.param = new(pb.Parameter)
	t.gradient = new(pb.Gradient)
	t.param.Value = int32(t.epoch)
	t.epoch = epoch
	t.fromChildren = make(map[uint64]*pb.Gradient)
	t.framework.FlagMeta(ctx, "Parents", "ParamReady")
}

// These are payload rpc for application purpose.
func (t *dummyMaster) ServeAsParent(fromID uint64, req string) ([]byte, error) {
	return json.Marshal(t.param)
}

func (t *dummyMaster) ServeAsChild(fromID uint64, req string) ([]byte, error) {
	return nil, nil
}

func (t *dummyMaster) CreateGRPCServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterRegressionServer(server, t)
	return server
}

func (t *dummyMaster) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	if linkType == "Children" {
		t.logger.Printf("master ChildMetaReady, task: %d, epoch: %d, child: %d\n", t.taskID, t.epoch, fromID)
		// Get data from child. When all the data is back, starts the next epoch.
		if meta == "GradientReady" {
			outputC := make(chan proto.Message, 1)
			t.framework.Fetch(ctx, fromID, "/proto.Regression/GetGradient", &pb.Input{t.epoch}, outputC)
			go t.ChildDataReady(ctx, fromID, meta, outputC)
		}
	}
}

func (t *dummyMaster) ChildDataReady(ctx context.Context, childID uint64, req string, outputC <-chan proto.Message) {
	// we need to select ctx cancel-chan later.
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

		t.logger.Printf("master ChildDataReady, task: %d, epoch: %d, child: %d, ready: %d\n",
			t.taskID, t.epoch, childID, len(t.fromChildren))

		// This is a weak form of checking. We can also check the task ids.
		// It means that we get all the events from children, and we
		// should go into the next epoch now.
		if len(t.fromChildren) == len(t.framework.GetTopology().GetNeighbors("Children", t.epoch)) {
			for _, g := range t.fromChildren {
				t.gradient.Value += g.Value
			}

			t.dataChan <- t.gradient.Value

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
