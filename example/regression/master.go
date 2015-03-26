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
// add error checking in the right places. We will skip these test for now.
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

	epochChange    chan *event
	getP           chan *event
	getG           chan *event
	childDataReady chan *event
	getGReqs       []*event
	exitChan       chan struct{}
}

type event struct {
	ctx      context.Context
	epoch    uint64
	input    *pb.Input
	retP     chan *pb.Parameter
	retG     chan *pb.Gradient
	gradient *pb.Gradient
	fromID   uint64
	output   proto.Message
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummyMaster) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.epochChange = make(chan *event, 1)
	t.getP = make(chan *event, 1)
	t.getG = make(chan *event, 1)
	t.childDataReady = make(chan *event, 1)
	t.exitChan = make(chan struct{})
	go t.run()
}

func (t *dummyMaster) run() {
	for {
		select {
		case et := <-t.epochChange:
			for _, gG := range t.getGReqs {
				close(gG.retG)
			}
			t.getGReqs = nil
			t.enterEpoch(et.ctx, et.epoch)
		case gP := <-t.getP:
			// We have to check epoch here in user level because grpc doesn't
			// allow use to intercept messages. This should be fixed later.
			err := t.framework.CheckEpoch(gP.input.Epoch)
			if err != nil {
				close(gP.retP)
			}
			gP.retP <- t.param
		case gG := <-t.getG:
			err := t.framework.CheckEpoch(gG.input.Epoch)
			if err != nil {
				close(gG.retG)
			}
			if t.gradient != nil {
				gG.retG <- t.gradient
				break
			}
			// Waiting queue. Requests will get notified later. The number of request
			// won't be huge presumingly.
			t.getGReqs = append(t.getGReqs, gG)
		case cr := <-t.childDataReady:
			t.ChildDataReady(cr.ctx, cr.fromID, cr.output)
		case <-t.exitChan:
			return
		}
	}
}

func (t *dummyMaster) Exit() {
	close(t.exitChan)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummyMaster) EnterEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("master SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	if t.testablyFail("SetEpoch", strconv.FormatUint(epoch, 10)) {
		return
	}
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (t *dummyMaster) enterEpoch(ctx context.Context, epoch uint64) {
	t.param = new(pb.Parameter)
	t.gradient = nil
	t.fromChildren = make(map[uint64]*pb.Gradient)

	t.epoch = epoch
	t.param.Value = int32(t.epoch)
	for _, c := range t.framework.GetTopology().GetNeighbors("Children", t.epoch) {
		t.framework.DataRequest(ctx, c, "/proto.Regression/GetGradient", &pb.Input{t.epoch})
	}
}

// These are payload rpc for application purpose.
func (t *dummyMaster) GetParameter(ctx context.Context, input *pb.Input) (*pb.Parameter, error) {
	retP := make(chan *pb.Parameter, 1)
	t.getP <- &event{ctx: ctx, input: input, retP: retP}
	p, ok := <-retP
	if !ok {
		return nil, fmt.Errorf("epoch changed")
	}
	return p, nil
}

func (t *dummyMaster) GetGradient(ctx context.Context, input *pb.Input) (*pb.Gradient, error) {
	retG := make(chan *pb.Gradient, 1)
	t.getP <- &event{ctx: ctx, input: input, retG: retG}
	g, ok := <-retG
	if !ok {
		return nil, fmt.Errorf("epoch changed")
	}
	return g, nil
}
func (t *dummyMaster) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	if method == "/proto.Regression/GetGradient" {
		t.childDataReady <- &event{ctx: ctx, fromID: fromID, output: output}
		return
	}
	panic("")
}

func (t *dummyMaster) gradientReady(ctx context.Context) {
	for _, gG := range t.getGReqs {
		gG.retG <- t.gradient
	}
	t.getGReqs = nil
	// In testing, we need to make sure dataChan has enough space and don't block.
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

func (t *dummyMaster) ChildDataReady(ctx context.Context, childID uint64, output proto.Message) {
	d, ok := output.(*pb.Gradient)
	if !ok {
		t.logger.Fatalf("Can't convert proto message to Gradient: %v", output)
	}
	t.fromChildren[childID] = d

	t.logger.Printf("master ChildDataReady, task: %d, epoch: %d, child: %d, ready: %d\n",
		t.taskID, t.epoch, childID, len(t.fromChildren))

	// This is a weak form of checking. We can also check the task ids.
	// But this really means that we get all the events from children, we
	// should go into the next epoch now.
	if len(t.fromChildren) == len(t.framework.GetTopology().GetNeighbors("Children", t.epoch)) {
		t.gradient = new(pb.Gradient)
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}
		t.gradientReady(ctx)
	}
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
	// Very hack. Need some internal knowledge. Don't change this.
	t.Exit()
	t.framework.Kill()
	t.NodeProducer <- true
	return true
}

func (t *dummyMaster) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {}
