package regression

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
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
