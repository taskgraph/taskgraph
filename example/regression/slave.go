package regression

import (
	"fmt"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/plutoshe/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/regression/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

	param        *pb.Parameter
	gradient     *pb.Gradient
	fromChildren map[uint64]*pb.Gradient

	epochChange chan *event
	getP        chan *event
	getG        chan *event
	pDataReady  chan *event
	gDataReady  chan *event
	getPReqs    []*event
	getGReqs    []*event
	exitChan    chan struct{}
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *dummySlave) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.epochChange = make(chan *event, 1)
	t.getP = make(chan *event, 1)
	t.getG = make(chan *event, 1)
	t.pDataReady = make(chan *event, 1)
	t.gDataReady = make(chan *event, 1)
	t.exitChan = make(chan struct{})
	go t.run()
}

func (t *dummySlave) run() {
	for {
		select {
		case ec := <-t.epochChange:
			for _, req := range t.getPReqs {
				close(req.retP)
			}
			t.getPReqs = nil
			for _, req := range t.getGReqs {
				close(req.retG)
			}
			t.getGReqs = nil

			t.enterEpoch(ec.ctx, ec.epoch)
		case req := <-t.getP:
			// We have to check epoch here in user level because grpc doesn't
			// allow use to intercept messages. This should be fixed later.
			err := t.framework.CheckGRPCContext(req.ctx)
			if err != nil {
				close(req.retP)
				break
			}
			if t.param != nil {
				req.retP <- t.param
				break
			}
			// Waiting queue. Requests will get notified later. The number of request
			// won't be huge presumingly.
			t.getPReqs = append(t.getPReqs, req)
		case req := <-t.getG:
			err := t.framework.CheckGRPCContext(req.ctx)
			if err != nil {
				close(req.retG)
				break
			}
			if t.gradient != nil {
				req.retG <- t.gradient
				break
			}
			// Waiting queue. Requests will get notified later. The number of request
			// won't be huge presumingly.
			t.getGReqs = append(t.getGReqs, req)
		case pr := <-t.pDataReady:
			t.ParentDataReady(pr.ctx, pr.fromID, pr.output)
		case gr := <-t.gDataReady:
			t.ChildDataReady(gr.ctx, gr.fromID, gr.output)
		case <-t.exitChan:
			return
		}
	}
}

func (t *dummySlave) Exit() {
	close(t.exitChan)
}

// This give the task an opportunity to cleanup and regroup.
func (t *dummySlave) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (t *dummySlave) enterEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("slave EnterEpoch, task %d, epoch %d\n", t.taskID, epoch)
	t.param = nil
	t.gradient = nil
	t.fromChildren = make(map[uint64]*pb.Gradient)
	t.epoch = epoch

	parent := t.framework.GetTopology()["Parents"].GetNeighbors(epoch)[0]
	t.framework.DataRequest(ctx, parent, "/proto.Regression/GetParameter", &pb.Input{})

	for _, c := range t.framework.GetTopology()["Children"].GetNeighbors(t.epoch) {
		t.framework.DataRequest(ctx, c, "/proto.Regression/GetGradient", &pb.Input{})
	}
}

func (t *dummySlave) GetParameter(ctx context.Context, input *pb.Input) (*pb.Parameter, error) {
	retP := make(chan *pb.Parameter, 1)
	t.getP <- &event{ctx: ctx, input: input, retP: retP}
	p, ok := <-retP
	if !ok {
		return nil, fmt.Errorf("epoch changed")
	}
	md, _ := metadata.FromContext(ctx)
	t.logger.Printf("slave serve GetParameter, task %d, from %s, epoch %s", t.taskID, md["taskID"], md["epoch"])
	return p, nil
}

func (t *dummySlave) GetGradient(ctx context.Context, input *pb.Input) (*pb.Gradient, error) {
	retG := make(chan *pb.Gradient, 1)
	t.getG <- &event{ctx: ctx, input: input, retG: retG}
	g, ok := <-retG
	if !ok {
		return nil, fmt.Errorf("epoch changed")
	}
	md, _ := metadata.FromContext(ctx)
	t.logger.Printf("slave serve GetGradient, task %d, from %s, epoch %s", t.taskID, md["taskID"], md["epoch"])
	return g, nil
}

func (t *dummySlave) parameterReady() {
	for _, req := range t.getPReqs {
		req.retP <- t.param
	}
	t.getPReqs = nil
}
func (t *dummySlave) gradientReady(ctx context.Context) {
	t.logger.Printf("slave gradient ready, task %d, epoch %d, gradient %d", t.taskID, t.epoch, t.gradient.Value)
	// If this failure happens, a new node will redo computing again.
	if t.testablyFail("ChildDataReady") {
		return
	}
	for _, req := range t.getGReqs {
		req.retG <- t.gradient
	}
	t.getGReqs = nil
	// if this failure happens, the parent could
	// 1. not get the data. DataRequest shall retry.
	// 2. already get the data. Where everything will continue and some work will drop
	//    if epoch bumps up.
	if t.testablyFail("ChildDataReady") {
		return
	}
}
func (t *dummySlave) checkGradReady(ctx context.Context) {
	children := t.framework.GetTopology()["Children"].GetNeighbors(t.epoch)
	if t.param != nil && len(t.fromChildren) == len(children) {
		t.gradient = new(pb.Gradient)
		t.gradient.Value = t.param.Value * int32(t.taskID)
		// In real ML, we add the gradient first.
		for _, g := range t.fromChildren {
			t.gradient.Value += g.Value
		}
		t.gradientReady(ctx)
	}
}

func (t *dummySlave) ParentDataReady(ctx context.Context, parentID uint64, output proto.Message) {
	t.logger.Printf("slave ParentDataReady, task %d, epoch %d, parent %d\n", t.taskID, t.epoch, parentID)
	if t.testablyFail("ParentDataReady") {
		return
	}
	d, ok := output.(*pb.Parameter)
	if !ok {
		t.logger.Fatalf("Can't convert proto message to Gradient: %v", output)
	}
	t.param = d
	t.parameterReady()
	t.checkGradReady(ctx)
}

func (t *dummySlave) ChildDataReady(ctx context.Context, childID uint64, output proto.Message) {
	d, ok := output.(*pb.Gradient)
	if !ok {
		t.logger.Fatalf("Can't convert proto message to Gradient: %v", output)
	}
	t.fromChildren[childID] = d

	t.logger.Printf("slave ChildDataReady, task %d, epoch %d, child %d, ready %d\n",
		t.taskID, t.epoch, childID, len(t.fromChildren))
	t.checkGradReady(ctx)
}

func (t *dummySlave) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	if method == "/proto.Regression/GetParameter" {
		t.pDataReady <- &event{ctx: ctx, fromID: fromID, output: output}
	} else {
		t.gDataReady <- &event{ctx: ctx, fromID: fromID, output: output}
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
	// Very hack. Need some internal knowledge. Don't change this.
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

func (t *dummySlave) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {}
