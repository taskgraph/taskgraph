package bwmf

import (
	"fmt"
	"io/ioutil"
	"log"
	// "math/rand"
	"os"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/op"
)

/* TODO:
- SetEpoch 0...
- Topology should be pushed to user. FlagMeta, Meta/Data Ready..
- Test Data. (Simple, real)
- FileSystem (hdfs, s3).. run real data.
*/

/*
The block wise matrix factorization task is designed for carry out block wise matrix
factorization for a variety of criteria (loss function) and constraints (nonnegativity
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
	numOfIters uint64

	fromOthers map[uint64]bool

	// The original data.
	namenodeAddr, webHdfsAddr, hdfsUser string
	rowShardPath, columnShardPath       string
	rowShard, columnShard               *pb.SparseMatrixShard

	shardedD, shardedT *pb.DenseMatrixShard
	fullD, fullT       *pb.DenseMatrixShard // appended shards

	// shardedD is size of m*k, t shard is size of k*n (but still its layed-out as n*k)
	// fullD is size of M*k, fullT is size of k*N (dittu, layout N*k)
	m, n, k, M, N int
	blockId       uint32

	// parameters for projected gradient methods
	sigma, alpha, beta, tol float32

	// Parameter data. They actually shares the underlying storage (buffer) with shardedD/T and fullD/T.
	shardedDParam, shardedTParam taskgraph_op.Parameter

	// objective function, parameters and minimizer to solve bwmf
	tLoss, dLoss *KLDivLoss
	optimizer    *taskgraph_op.ProjectedGradient
	stopCriteria taskgraph_op.StopCriteria

	// channels
	epochChange chan *event
	getD        chan *event
	getT        chan *event
	tDataReady  chan *event
	dDataReady  chan *event
	exitChan    chan struct{}
}

// These two function carry out actual optimization.
func (bt *bwmfTask) updateDShard() {

	// TODO(baigang): set dLoss

	ok := bt.optimizer.Minimize(bt.dLoss, bt.stopCriteria, bt.shardedDParam)
	if !ok {
		// TODO report error
	}

}

func (bt *bwmfTask) updateTShard() {

	// TODO(baigang): set tLoss

	ok := bt.optimizer.Minimize(bt.tLoss, bt.stopCriteria, bt.shardedTParam)
	if !ok {
		// TODO report error
	}

	// TODO copy data from shardedTParam to shardedT
}

func (bt *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	bt.taskID = taskID
	bt.framework = framework
	bt.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	// Below is loading the file. NOTE: this should be moved to a separate function.
	hdfsClient, hcOk := filesystem.NewHdfsClient(bt.namenodeAddr, bt.webHdfsAddr, bt.hdfsUser)
	if hcOk != nil {
		bt.logger.Fatal("Failed connecting to HDFS at ", bt.namenodeAddr, bt.webHdfsAddr, ", with account ", bt.hdfsUser)
	}
	rowShardReader, rsrOk := hdfsClient.OpenReadCloser(bt.rowShardPath)
	if rsrOk != nil {
		bt.logger.Fatalf("Failed load matrix data on task: %d, form path %s", bt.taskID, bt.rowShardPath)
	}
	columnShardReader, csrOk := hdfsClient.OpenReadCloser(bt.columnShardPath)
	if csrOk != nil {
		bt.logger.Fatalf("Failed load matrix data on task: %d, from path %s", bt.taskID, bt.columnShardPath)
	}

	rowBuf, rbOk := ioutil.ReadAll(rowShardReader)
	if rbOk != nil {
		bt.logger.Fatalf("Failed read rowSharded data on task: %d", bt.taskID)
	}

	columnBuf, cbOk := ioutil.ReadAll(columnShardReader)
	if cbOk != nil {
		bt.logger.Fatalf("Failed load columnSharded data on task: %d", bt.taskID)
	}

	rowUnmashalErr := proto.Unmarshal(rowBuf, bt.rowShard)
	columnUnmashalErr := proto.Unmarshal(columnBuf, bt.rowShard)

	// XXX(baigang) We set M and N via collecting all sharded D and T.
	// XXX(baigang) K is set in the task builder.
	bt.m = len(bt.rowShard.Row)
	bt.n = len(bt.columnShard.Row)

	// TODO we also need start indices for sharded D and T.
	bt.shardedD = &pb.DenseMatrixShard{
		Row: make([]*pb.DenseMatrixShard_DenseRow, bt.m),
	}
	bt.shardedT = &pb.DenseMatrixShard{
		Row: make([]*pb.DenseMatrixShard_DenseRow, bt.n),
	}
	for i, _ := range bt.shardedD.Row {
		bt.shardedD.Row[i].At = make([]float32, bt.k)
	}
	for i, _ := range bt.shardedT.Row {
		bt.shardedT.Row[i].At = make([]float32, bt.k)
	}

	bt.shardedDParam = NewBlockParameter(bt.shardedD)
	bt.shardedTParam = NewBlockParameter(bt.shardedT)

	// TODO set tLoss and dLoss
	bt.tLoss = &KLDivLoss{
		V:      nil,
		WH:     nil,
		W:      nil,
		m:      bt.m,
		n:      bt.n,
		k:      bt.k,
		smooth: 1e-3,
	}
	bt.dLoss = &KLDivLoss{
		V:      nil,
		WH:     nil,
		W:      nil,
		m:      bt.m,
		n:      bt.n,
		k:      bt.k,
		smooth: 1e-3,
	}

	bt.stopCriteria = taskgraph_op.MakeFixCountStopCriteria(int(bt.numOfIters))
	proj_len := 0
	if bt.m > bt.n {
		proj_len = bt.m * bt.k
	} else {
		proj_len = bt.n * bt.k
	}

	bt.optimizer = taskgraph_op.NewProjectedGradient(
		taskgraph_op.NewProjection(
			taskgraph_op.NewAllTheSameParameter(1e20, proj_len),
			taskgraph_op.NewAllTheSameParameter(1e-9, proj_len),
		),
		bt.beta,
		bt.sigma,
		bt.alpha,
	)

	// channels for event-based `run()`
	bt.epochChange = make(chan *event, 1)
	bt.getD = make(chan *event, 1)
	bt.getT = make(chan *event, 1)
	bt.tDataReady = make(chan *event, 1)
	bt.dDataReady = make(chan *event, 1)
	bt.exitChan = make(chan struct{})

	// Now we have
	go bt.run()
}

type event struct {
	ctx      context.Context
	epoch    uint64
	fromID   uint64
	request  *pb.Request
	retT     chan *pb.DenseMatrixShard
	retD     chan *pb.DenseMatrixShard
	response proto.Message
	// to be extended
}

func (bt *bwmfTask) run() {
	for {
		select {
		case ec := <-bt.epochChange:
			bt.doEnterEpoch(ec.ctx, ec.epoch)
		case reqT := <-bt.getT:
			// do getT here, should be synchronous so we won't arrive at race condition
			// also we assume that all D shards are ready here
			bt.updateTShard()
			reqT.retT <- bt.shardedT
		case reqD := <-bt.getD:
			// do getD here. Dittu as reqT
			bt.updateDShard()
			reqD.retD <- bt.shardedD

		case dr := <-bt.tDataReady:
			// each event comes as one shard is ready
			// TODO: we need organize all the shards according to blockIds
			// In other words, put each Response into fullT

		case dr := <-bt.dDataReady:
			// Dittu. Put response into fullD

		case <-bt.exitChan:
			return
		}
	}
}

func (bt *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	bt.epochChange <- &event{ctx: ctx, epoch: epoch}
}
func (bt *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	bt.logger.Printf("master EnterEpoch, task %d, epoch %d\n", bt.taskID, epoch)
	bt.epoch = epoch

}

// XXX(baigang): We do not have to get notified. We just send the request and wait for the response via grpc.
func (bt *bwmfTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	// XXX intentional empty
}

func (bt *bwmfTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	switch method {
	case "/proto.BlockData/GetTShard":
		bt.tDataReady <- &event{ctx: ctx, fromID: fromID, response: output}
	case "/proto.BlockData/GetDShard":
		bt.dDataReady <- &event{ctx: ctx, fromID: fromID, response: output}
	default:
		bt.logger.Panicf("Unknow data method: %s", method)
	}
}

func (bt *bwmfTask) CreateServer() *grpc.Server {
	server := grpc.NewServer()
	pb.RegisterBlockDataServer(server, bt)
	return server
}

// NOTE: what's thsi
func (bt *bwmfTask) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.DataBlock/GetTShard":
		return &pb.Response{BlockId: bt.blockId, Shard: bt.shardedT}
	case "/proto.DataBlock/GetDShard":
		return &pb.Response{BlockId: bt.blockId, Shard: bt.shardedD}
	default:
		bt.logger.Panicf("Unknown method: %s", methodName)
	}

	// XXX make the compiler happy
	panic("")
}

func (bt *bwmfTask) Exit() {
	close(bt.exitChan)
	// XXX Shall we dump the temporary results here or the last epoch?
}

// Implement of the DataBlock service described in proto.
func (bt *bwmfTask) GetTShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// XXX(baigang): here suppose tShard has already been optimized and stored in it

	// Here basically we send a getT request to the event channel. Then the `Run()` will handle it and
	// calculates the tShard, during which we just wait here. The result will be sent back to channel
	// retT, and we fetch it here and respond it via grpc.

	retT := make(chan *pb.DenseMatrixShard, 1)
	bt.getT <- &event{ctx: ctx, request: request, retT: retT}

	resT, ok := <-retT
	if !ok {
		return nil, fmt.Errorf("Failed getting T.")
	}

	// TODO & NOTE: create it here or use the one returned by CreateOutputMessage
	return &pb.Response{BlockId: bt.blockId, Shard: resT}, nil
}

func (bt *bwmfTask) GetDShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// XXX(baigang): here suppose tShard has already been optimized and stored in it

	// same as GetTShard
	retD := make(chan *pb.DenseMatrixShard, 1)
	bt.getD <- &event{ctx: ctx, request: request, retD: retD}

	resD, ok := <-retD
	if !ok {
		return nil, fmt.Errorf("Failed getting D.")
	}
	return &pb.Response{BlockId: bt.blockId, Shard: resD}, nil
}
