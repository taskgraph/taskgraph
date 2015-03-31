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
We will have `numOfTasks` tasks that handle both row task and column task in alternation. Each task
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
	fullD, fullT       map[uint32]*pb.DenseMatrixShard // blockId to Shard

	// shardedD is size of m*k, t shard is size of k*n (but still its layed-out as n*k)
	// fullD is size of M*k, fullT is size of k*N (dittu, layout N*k)
	m, n, k, M, N int
	blockId       uint32

	// parameters for projected gradient methods
	sigma, alpha, beta, tol float32

	// Parameter data. They actually shares the underlying storage (buffer) with shardedD/T.
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
	tDone       chan *event
	dDone       chan *event
	exitChan    chan struct{}
}

// These two function carry out actual optimization.
func (bt *bwmfTask) updateDShard() {

	// fullT is ready here

	// if dLoss is not initialized
	if bt.dLoss.n == -1 {
		bt.dLoss.n = 0
		for _, m := range bt.fullT {
			bt.dLoss.n += len(m.Row)
		}
		bt.N = bt.dLoss.n
		wh := make([][]float32, bt.N)
		for i, _ := range wh {
			wh[i] = make([]float32, bt.m)
		}
		bt.dLoss.WH = wh
	}
	bt.dLoss.W = NewBlocksParameter(&bt.fullT)

	// all set. Start the optimizing routine.
	ok := bt.optimizer.Minimize(bt.dLoss, bt.stopCriteria, bt.shardedDParam)
	if !ok {
		// TODO report error
	}

	// signal that dShard has already been evaluated
	bt.dDone <- &event{epoch: bt.epoch}
}

func (bt *bwmfTask) updateTShard() {
	// Symmetric as updateDShard
	if bt.tLoss.m == -1 {
		bt.tLoss.m = 0
		for _, m := range bt.fullD {
			bt.tLoss.m += len(m.Row)
		}
		bt.M = bt.tLoss.m
		wh := make([][]float32, bt.M)
		for i, _ := range wh {
			wh[i] = make([]float32, bt.n)
		}
		bt.tLoss.WH = wh
	}
	bt.tLoss.W = NewBlocksParameter(&bt.fullD)
	ok := bt.optimizer.Minimize(bt.tLoss, bt.stopCriteria, bt.shardedTParam)
	if !ok {
		// TODO report error
	}
	bt.tDone <- &event{epoch: bt.epoch}
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

	if rowUnmashalErr != nil {
		bt.logger.Fatalf("Failed unmarshalling row shard data on task: %d", bt.taskID)
	}
	if columnUnmashalErr != nil {
		bt.logger.Fatalf("Failed unmarshalling column shard data on task: %d", bt.taskID)
	}

	// XXX(baigang) We set M and N via collecting all sharded D and T.
	// XXX(baigang) K is set in the task builder.
	bt.m = len(bt.rowShard.Row)
	bt.n = len(bt.columnShard.Row)

	bt.shardedD = &pb.DenseMatrixShard{
		Row: make([]*pb.DenseMatrixShard_DenseRow, bt.m),
	}
	bt.shardedT = &pb.DenseMatrixShard{
		Row: make([]*pb.DenseMatrixShard_DenseRow, bt.n),
	}
	for i, _ := range bt.shardedD.Row {
		bt.shardedD.Row[i].At = make([]float32, bt.k)
		// TODO random fill
	}
	for i, _ := range bt.shardedT.Row {
		bt.shardedT.Row[i].At = make([]float32, bt.k)
		// TODO random fill
	}

	bt.shardedDParam = NewSingleBlockParameter(bt.shardedD)
	bt.shardedTParam = NewSingleBlockParameter(bt.shardedT)

	// TODO set tLoss and dLoss
	bt.tLoss = &KLDivLoss{
		V:      bt.columnShard,
		WH:     nil,
		W:      nil,
		m:      -1, // should be bt.M
		n:      bt.n,
		k:      bt.k,
		smooth: 1e-4,
	}
	bt.dLoss = &KLDivLoss{
		V:      bt.rowShard,
		WH:     nil,
		W:      nil,
		m:      bt.m,
		n:      -1, // should be bt.N
		k:      bt.k,
		smooth: 1e-4,
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
	bt.tDone = make(chan *event, 1)
	bt.dDone = make(chan *event, 1)
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
			// XXX wait for shardedT for current epoch
			ee := <-bt.tDone
			if ee.epoch == reqT.epoch {
				err := bt.framework.CheckGRPCContext(reqT.ctx)
				if err != nil {
					close(reqT.retT)
					break
				}
				reqT.retT <- bt.shardedT
			}

		case reqD := <-bt.getD:
			// XXX Dittu, wait for shardedD for current epoch
			ee := <-bt.dDone
			if ee.epoch == reqD.epoch {
				err := bt.framework.CheckGRPCContext(reqD.ctx)
				if err != nil {
					close(reqD.retD)
					break
				}
				reqD.retD <- bt.shardedD
			}

		case dr := <-bt.tDataReady:
			// each event comes as one shard is ready
			// TODO: we need organize all the shards according to blockIds
			// In other words, put each Response into fullT
			resp, bOk := dr.response.(*pb.Response)
			if !bOk {
				bt.logger.Fatalf("Cannot convert proto message to Response: %v", dr.response)
			}
			_, exists := bt.fullT[resp.BlockId]
			if exists {
				bt.logger.Fatalf("Duplicated response from block %d for full T shard  with response: %v", resp.BlockId, dr.response)
			}
			bt.fullT[resp.BlockId] = resp.Shard

			// if all tShards have arrived
			if len(bt.fullT) == int(bt.numOfTasks) {
				// optimize to evaluate dShard. It will signals via bt.dDone at completion.
				go bt.updateDShard()
			}

		case dr := <-bt.dDataReady:
			// Dittu. Put response into fullD
			resp, bOk := dr.response.(*pb.Response)
			if !bOk {
				bt.logger.Fatalf("Cannot convert proto message to Response: %v", dr.response)
			}
			_, exists := bt.fullD[resp.BlockId]
			if exists {
				bt.logger.Fatalf("Duplicated response from block %d for full D shard  with response: %v", resp.BlockId, dr.response)
			}
			bt.fullD[resp.BlockId] = resp.Shard

			// if all tShards have arrived
			if len(bt.fullD) == int(bt.numOfTasks) {
				// optimize to evaluate dShard. It will signals via bt.tDone at completion.
				go bt.updateTShard()
			}

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

	// reset the data blocks
	bt.fullD = make(map[uint32]*pb.DenseMatrixShard)
	bt.fullT = make(map[uint32]*pb.DenseMatrixShard)

	// ask for
	var method string
	if epoch%2 == 0 {
		method = "/proto.BlockData/GetDShard"
		// XXX: Add own block to it because neighbors doesn't include self.
		bt.fullD[bt.blockId] = bt.shardedD
	} else {
		method = "proto.BlockData/GetTShard"
		// dittu XXX.
		bt.fullT[bt.blockId] = bt.shardedT
	}

	for _, c := range bt.framework.GetTopology().GetNeighbors("Children", epoch) {
		bt.framework.DataRequest(ctx, c, method, &pb.Request{Epoch: epoch})
	}
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

func (bt *bwmfTask) CreateOutputMessage(methodName string) proto.Message {
	switch methodName {
	case "/proto.BlockData/GetTShard":
		return new(pb.Response)
	case "/proto.BlockData/GetDShard":
		return new(pb.Response)
	default:
		bt.logger.Panicf("Unknown method: %s", methodName)
	}

	// make the compiler happy
	panic("")
}

func (bt *bwmfTask) Exit() {
	close(bt.exitChan)
	// XXX Shall we dump the temporary results here or the last epoch?
}

// Implement of the BlockData service described in proto.
func (bt *bwmfTask) GetTShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// XXX(baigang): here suppose tShard has already been optimized and stored in it

	// Here basically we send a getT request to the event channel. Then the `Run()` will handle it and
	// calculates the tShard, during which we just wait here. The result will be sent back to channel
	// retT, and we fetch it here and respond it via grpc.

	retT := make(chan *pb.DenseMatrixShard, 1)
	bt.getT <- &event{ctx: ctx, epoch: request.Epoch, request: request, retT: retT}

	resT, ok := <-retT
	if !ok {
		return nil, fmt.Errorf("Failed getting T.")
	}

	return &pb.Response{BlockId: bt.blockId, Shard: resT}, nil
}

func (bt *bwmfTask) GetDShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// XXX(baigang): here suppose tShard has already been optimized and stored in it

	// same as GetTShard
	retD := make(chan *pb.DenseMatrixShard, 1)
	bt.getD <- &event{ctx: ctx, epoch: request.Epoch, request: request, retD: retD}

	resD, ok := <-retD
	if !ok {
		return nil, fmt.Errorf("Failed getting D.")
	}
	return &pb.Response{BlockId: bt.blockId, Shard: resD}, nil
}
