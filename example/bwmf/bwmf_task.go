package bwmf

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

/*
The block wise matrix factorization task is designed for carry out block wise matrix
factorization for a variety of criteria (loss function) and constraints (non-negativity
for example).

The main idea behind the bwmf is following:
We will have K tasks that handle both row task and column task in alternation. Each task
will read two copies of the data: one row shard and one column shard of A. It either hosts
one shard of D and a full copy of T, or one shard of T and a full copy of D, depending on
the epoch of iteration. "A full copy" consists of computation results from itself and
all "children".
Topology: the topology is different from task to task. Each task will consider itself parent
and all others children.
*/

// bwmfTasks holds two shards of original matrices (row and column), one shard of D,
// and one shard of T. It works differently for odd and even epoch:
// During odd epoch, 1. it fetch all T from other slaves, and finding better value for
// local shard of D; 2. after it is done, it let every one knows. Vice versa for even epoch.
// Task 0 will monitor the progress and responsible for starting the work of new epoch.
type bwmfTask struct {
	framework  taskgraph.Framework
	epoch      uint64
	taskID     uint64
	logger     *log.Logger
	numOfTasks uint64

	peerShardReady map[uint64]bool
	peerUpdated    map[uint64]bool
	// event hanlding
	epochChange chan *event
	getT        chan *event
	getD        chan *event
	dataReady   chan *event
	updateDone  chan *event
	metaReady   chan *event
	exitChan    chan *event
}

type event struct {
	ctx     context.Context
	epoch   uint64
	request *pb.Request
	retT    chan *pb.Response
	retD    chan *pb.Response
	fromID  uint64
	method  string
	output  proto.Message
}

func (t *bwmfTask) initDShard()             {}
func (t *bwmfTask) initTShard()             {}
func (t *bwmfTask) getDShard() *pb.Response { return &pb.Response{} }
func (t *bwmfTask) getTShard() *pb.Response { return &pb.Response{} }

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.initDShard()
	t.initTShard()

	t.epochChange = make(chan *event, 1)
	t.getT = make(chan *event, 1)
	t.getD = make(chan *event, 1)
	t.dataReady = make(chan *event, 1)
	t.updateDone = make(chan *event, 1)
	t.metaReady = make(chan *event, 1)
	t.exitChan = make(chan *event)
	go t.run()
}

func (t *bwmfTask) run() {
	for {
		select {
		case epochChange := <-t.epochChange:
			t.doEnterEpoch(epochChange.ctx, epochChange.epoch)
		case req := <-t.getT:
			t.logger.Printf("trying to serve T shard, task %d, epoch %d", t.taskID, t.epoch)
			err := t.framework.CheckGRPCContext(req.ctx)
			if err != nil {
				close(req.retT)
				break
			}
			// We only return the data shard of previous epoch. So it always exists.
			req.retT <- t.getTShard()
		case req := <-t.getD:
			t.logger.Printf("trying to serve D shard, task %d, epoch %d", t.taskID, t.epoch)
			err := t.framework.CheckGRPCContext(req.ctx)
			if err != nil {
				close(req.retD)
				break
			}
			req.retD <- t.getDShard()
		case dataReady := <-t.dataReady:
			t.doDataReady(dataReady.ctx, dataReady.fromID, dataReady.method, dataReady.output)
		case done := <-t.updateDone:
			t.notifyMaster(done.ctx)
		case notify := <-t.metaReady:
			t.notifyUpdate(notify.ctx, notify.fromID)
		case <-t.exitChan:
			return
		}
	}
}

func (t *bwmfTask) Exit() {
	close(t.exitChan)
}

func (t *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (t *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("doEnterEpoch, task %d, epoch %d", t.taskID, epoch)
	t.peerShardReady = make(map[uint64]bool)
	t.peerUpdated = make(map[uint64]bool)
	t.epoch = epoch
	if epoch%2 == 0 {
		t.fetchShards(ctx, "/proto.BlockData/GetDShard")
	} else {
		t.fetchShards(ctx, "/proto.BlockData/GetTShard")
	}
}

func (t *bwmfTask) fetchShards(ctx context.Context, method string) {
	peers := t.framework.GetTopology().GetNeighbors("Neighbors", t.epoch)
	for _, peer := range peers {
		t.framework.DataRequest(ctx, peer, method, &pb.Request{})
	}
}

func (t *bwmfTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.dataReady <- &event{ctx: ctx, fromID: fromID, method: method, output: output}
}
func (t *bwmfTask) doDataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	t.logger.Printf("doDataReady, task %d, from %d, epoch %d, method %s", t.taskID, fromID, t.epoch, method)
	t.peerShardReady[fromID] = true
	if len(t.peerShardReady) == int(t.numOfTasks) {
		if t.epoch%2 == 0 {
			t.logger.Printf("Full D ready, task %d, epoch %d", t.taskID, t.epoch)
			go t.updateTShard(ctx)
		} else {
			t.logger.Printf("Full T ready, task %d, epoch %d", t.taskID, t.epoch)
			go t.updateDShard(ctx)
		}
	}
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard(ctx context.Context) {
	select {
	case <-time.After(time.Duration(rand.Intn(3)) * time.Second):
	case <-ctx.Done():
		return
	}
	t.updateDone <- &event{ctx: ctx}
}
func (t *bwmfTask) updateTShard(ctx context.Context) {
	select {
	case <-time.After(time.Duration(rand.Intn(3)) * time.Second):
	case <-ctx.Done():
		return
	}
	t.updateDone <- &event{ctx: ctx}
}

func (t *bwmfTask) notifyMaster(ctx context.Context) {
	t.framework.FlagMeta(ctx, "Master", "done")
}

func (t *bwmfTask) CreateOutputMessage(method string) proto.Message {
	switch method {
	case "/proto.BlockData/GetDShard":
		return new(pb.Response)
	case "/proto.BlockData/GetTShard":
		return new(pb.Response)
	}
	panic("")
}

func (t *bwmfTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterBlockDataServer(server, t)
	return server
}

func (t *bwmfTask) GetTShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	retT := make(chan *pb.Response, 1)
	t.getT <- &event{ctx: ctx, request: request, retT: retT}
	resp, ok := <-retT
	if !ok {
		return nil, fmt.Errorf("epoch changed!")
	}
	return resp, nil
}

func (t *bwmfTask) GetDShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	retD := make(chan *pb.Response, 1)
	t.getD <- &event{ctx: ctx, request: request, retD: retD}
	resp, ok := <-retD
	if !ok {
		return nil, fmt.Errorf("epoch changed!")
	}
	return resp, nil
}

func (t *bwmfTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	t.metaReady <- &event{ctx: ctx, fromID: fromID}
}

func (t *bwmfTask) notifyUpdate(ctx context.Context, fromID uint64) {
	t.logger.Printf("notifyUpdate, task %d, from %d, epoch %d", t.taskID, fromID, t.epoch)
	t.peerUpdated[fromID] = true
	if len(t.peerUpdated) == int(t.numOfTasks) {
		t.logger.Printf("All tasks update done, epoch %d", t.epoch)
		if t.epoch < 2 {
			t.framework.IncEpoch(ctx)
		} else {
			t.framework.ShutdownJob()
		}
	}
}
