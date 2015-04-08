package bwmf

import (
	"bufio"
	"fmt"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/op"
)

/*
The block wise matrix factorization task is designed for carry out block wise matrix
factorization for a variety of criteria (loss function) and constraints (non-negativity
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
	framework taskgraph.Framework
	epoch     uint64
	taskID    uint64

	workPath  string
	logger    *log.Logger
	logWriter *bufio.Writer

	numOfTasks uint64
	numOfIters uint64
	curIter    uint64

	fromOthers map[uint64]bool

	// The original data.
	rowBuf, columnBuf     []byte
	rowShard, columnShard *pb.SparseMatrixShard

	shardedD, shardedT *pb.DenseMatrixShard
	fullD, fullT       map[uint32]*pb.DenseMatrixShard // blockId to Shard

	// shardedD is size of m*k, t shard is size of k*n (but still its layed-out as n*k)
	// fullD is size of M*k, fullT is size of k*N (dittu, layout N*k)
	m, n, k, M, N int
	blockId       uint32

	// parameters for projected gradient methods
	sigma, alpha, beta, tol float32

	// objective function, parameters and minimizer to solve bwmf
	tLoss, dLoss *KLDivLoss
	optimizer    *taskgraph_op.ProjectedGradient
	stopCriteria taskgraph_op.StopCriteria

	// channels
	epochChange chan *event
	getD        chan *event
	getT        chan *event
	dataReady   chan *event
	metaReady   chan *event
	updateDone  chan *event
	exitChan    chan struct{}
}

// These two function carry out actual optimization.
func (bt *bwmfTask) updateDShard(ctx context.Context) {
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

	param := NewSingleBlockParameter(bt.shardedD)

	// all set. Start the optimizing routine.
	ok := bt.optimizer.Minimize(bt.dLoss, bt.stopCriteria, param)
	if !ok {
		// TODO report error
	}

	// signal that dShard has already been evaluated
	bt.updateDone <- &event{ctx: ctx, epoch: bt.epoch}
}

func (bt *bwmfTask) updateTShard(ctx context.Context) {
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
	param := NewSingleBlockParameter(bt.shardedT)
	ok := bt.optimizer.Minimize(bt.tLoss, bt.stopCriteria, param)
	if !ok {
		// TODO report error
	}
	bt.updateDone <- &event{ctx: ctx, epoch: bt.epoch}
}

// Dumping the temporary results.
func (bt *bwmfTask) checkpoint(epoch uint64) {
	var (
		buf      []byte
		err      error
		filename string
	)

	// In even epoch, we fetch D and update T; in odd epoch, we fetch T and update D.
	if epoch%2 == 0 {
		filename = "./shard.t." + strconv.FormatInt(int64(bt.taskID), 10) + "-" + strconv.FormatInt(int64(epoch), 10)
		buf, err = proto.Marshal(bt.shardedT)
	} else {
		filename = "./shard.d." + strconv.FormatInt(int64(bt.taskID), 10) + "-" + strconv.FormatInt(int64(epoch), 10)
		buf, err = proto.Marshal(bt.shardedD)
	}
	if err != nil {
		bt.logger.Panicf("Failed marshalling result at epoch %d in task %d with err %s", epoch, bt.taskID, err)
	}
	// write the buffer
	writer, openErr := filesystem.NewLocalFSClient().OpenWriteCloser(filename)
	if openErr != nil {
		bt.logger.Panicf("Failed open %s with error %s.", filename, openErr)
	}
	_, wErr := writer.Write(buf)
	if wErr != nil {
		bt.logger.Panicf("Failed writing to %s with err %s", filename, wErr)
	}

	// remove the older instance
	if int64(epoch)-2 >= 0 {
		// TODO: No `Remove` interface for now.
		// preEpoch := epoch - 2
	}
}

// load the dumped result from checkpoint.
// TODO: failure recovery using this func
func (bt *bwmfTask) recoverFromCheckpoint(epoch uint64) {
	var (
		filename string
		shard    *pb.DenseMatrixShard
	)

	// In even epoch, we fetch D and update T; in odd epoch, we fetch T and update D.
	if epoch%2 == 0 {
		filename = "./shard.t.checkpoint." + strconv.FormatInt(int64(bt.taskID), 10) + "-" + strconv.FormatInt(int64(epoch), 10)
		shard = bt.shardedT
	} else {
		filename = "./shard.d.checkpoint." + strconv.FormatInt(int64(bt.taskID), 10) + "-" + strconv.FormatInt(int64(epoch), 10)
		shard = bt.shardedD
	}

	// load the buffer
	client := NewLocalBufLoader()
	client.Init()
	buf, err := client.ReadAll(filename)
	if err != nil {
		bt.logger.Panicf("Failed loading file %s", filename)
	}
	err = proto.Unmarshal(buf, shard)
	if err != nil {
		bt.logger.Panicf("Failed unmarshalling result at epoch %d in task %d", epoch, bt.taskID)
	}
}

func (bt *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	bt.taskID = taskID
	bt.framework = framework
	bt.curIter = 0

	// XXX This is for exposing the io.Writer for us to constantly flush.
	logFile, lfErr := os.Create(bt.workPath + "/task-" + strconv.FormatInt(int64(taskID), 10) + ".log")
	if lfErr != nil {
		panic("Failed creating logger.")
	}
	bt.logWriter = bufio.NewWriter(logFile)
	bt.logger = log.New(bt.logWriter, "", log.Ldate|log.Ltime|log.Lshortfile)
	bt.logger.Println("Start logging.")
	go func() {
		// constantly flushing the log messages.
		for {
			bt.logWriter.Flush()
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// just for mem allocation
	bt.rowShard = &pb.SparseMatrixShard{}
	bt.columnShard = &pb.SparseMatrixShard{}
	rowUnmarshalErr := proto.Unmarshal(bt.rowBuf, bt.rowShard)
	columnUnmarshalErr := proto.Unmarshal(bt.columnBuf, bt.columnShard)

	if rowUnmarshalErr != nil {
		bt.logger.Fatalf("Failed unmarshalling row shard data on task: %d", bt.taskID)
	}
	if columnUnmarshalErr != nil {
		bt.logger.Fatalf("Failed unmarshalling column shard data on task: %d", bt.taskID)
	}

	// XXX(baigang) We set M and N via collecting all sharded D and T later.
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
		bt.shardedD.Row[i] = &pb.DenseMatrixShard_DenseRow{At: make([]float32, bt.k)}
		for j, _ := range bt.shardedD.Row[i].At {
			bt.shardedD.Row[i].At[j] = rand.Float32()
		}
	}
	for i, _ := range bt.shardedT.Row {
		bt.shardedT.Row[i] = &pb.DenseMatrixShard_DenseRow{At: make([]float32, bt.k)}
		for j, _ := range bt.shardedT.Row[i].At {
			bt.shardedT.Row[i].At[j] = rand.Float32()
		}
	}

	// TODO set tLoss and dLoss
	bt.tLoss = &KLDivLoss{
		V:      bt.columnShard,
		WH:     nil,
		W:      nil,
		m:      -1, // should be bt.M
		n:      bt.n,
		k:      bt.k,
		smooth: 1e-4,
		logger: bt.logger,
	}
	bt.dLoss = &KLDivLoss{
		V:      bt.rowShard,
		WH:     nil,
		W:      nil,
		m:      bt.m,
		n:      -1, // should be bt.N
		k:      bt.k,
		smooth: 1e-4,
		logger: bt.logger,
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
	bt.dataReady = make(chan *event, 1)
	bt.metaReady = make(chan *event, 1)
	bt.updateDone = make(chan *event, 1)
	bt.exitChan = make(chan struct{})

	bt.logger.Println("RowShard:", *bt.rowShard)
	bt.logger.Println("ColumnShard:", *bt.columnShard)
	bt.logger.Println("m: ", bt.m)
	bt.logger.Println("n: ", bt.n)
	bt.logger.Println("k: ", bt.k)
	bt.logger.Println("blockId: ", bt.blockId)
	bt.logger.Println("sharded T: ", bt.shardedT)
	bt.logger.Println("sharded D: ", bt.shardedD)
	bt.logWriter.Flush()

	// Now we have everything initialized.
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
			bt.logger.Println("SELECT- Epoch change to ", ec.epoch, " at task ", bt.taskID)
			bt.doEnterEpoch(ec.ctx, ec.epoch)

		case reqT := <-bt.getT:
			bt.logger.Println("SELECT- GetT at epoch", bt.epoch)
			// XXX: tShard is guaranteed to have been updated here.
			err := bt.framework.CheckGRPCContext(reqT.ctx, bt.logger)
			if err != nil {
				bt.logger.Panicf("GetTShard grpc broken in context at task %d for epoch %d.", bt.taskID, bt.epoch)
				close(reqT.retT)
			}
			bt.logger.Printf("To respond with retT at task %d epoch %d.", bt.taskID, bt.epoch)
			reqT.retT <- bt.shardedT
			bt.logger.Printf("Responded with retT at task %d epoch %d.", bt.taskID, bt.epoch)
		case reqD := <-bt.getD:
			bt.logger.Println("SELECT- GetD at epoch", bt.epoch)
			// XXX: tShard is guaranteed to have been updated here.

			bt.logger.Println("Before checking GRPC status...")
			bt.logWriter.Flush()
			err := bt.framework.CheckGRPCContext(reqD.ctx, bt.logger)
			bt.logger.Println("After checking GRPC status: ", err)
			bt.logWriter.Flush()
			if err != nil {
				bt.logger.Panicf("GetDShard grpc broken in context at task %d for epoch %d.", bt.taskID, bt.epoch)
				close(reqD.retD)
				continue
			}
			bt.logger.Printf("To respond with retD at task %d epoch %d.", bt.taskID, bt.epoch)
			reqD.retD <- bt.shardedD
			bt.logger.Printf("Responded with retD at task %d epoch %d.", bt.taskID, bt.epoch)
		case done := <-bt.updateDone:
			bt.logger.Println("SELECT- update t/d is done at epoch", bt.epoch)
			go bt.checkpoint(bt.epoch)
			// bt.framework.FlagMeta(done.ctx, "toMaster", "metaReady")
			bt.logger.Println("Flagging metaReady.")
			bt.framework.FlagMeta(done.ctx, "Neighbors", "metaReady")
		case dr := <-bt.dataReady:
			bt.logger.Println("SELECT- DataReady at epoch", bt.epoch)
			// each event comes as the shard is ready
			resp, bOk := dr.response.(*pb.Response)
			if !bOk {
				bt.logger.Fatalf("Cannot convert proto message to Response: %v", dr.response)
			}
			// In even epoch, we fetch all D shards and update T; while in odd epoch, we fetch T and update D.
			if bt.epoch%2 == 0 {
				_, exists := bt.fullD[resp.BlockId]
				if exists {
					bt.logger.Fatalf("Duplicated response from block %d for full D shard  with response: %v", resp.BlockId, dr.response)
				}
				bt.fullD[resp.BlockId] = resp.Shard
				// if all tShards have arrived
				if len(bt.fullD) == int(bt.numOfTasks) {
					// optimize to evaluate dShard. It will signals via bt.updateDone at completion.
					bt.logger.Println("FullD is ready. data: ", bt.fullD)
					go bt.updateTShard(dr.ctx)
				}
			} else {
				_, exists := bt.fullT[resp.BlockId]
				if exists {
					bt.logger.Fatalf("Duplicated response from block %d for full T shard  with response: %v", resp.BlockId, dr.response)
				}
				bt.fullT[resp.BlockId] = resp.Shard
				// if all tShards have arrived
				if len(bt.fullT) == int(bt.numOfTasks) {
					// optimize to evaluate dShard. It will signals via bt.updateDone at completion.
					bt.logger.Println("FullT is ready. data: ", bt.fullT)
					go bt.updateDShard(dr.ctx)
				}
			}

		case mr := <-bt.metaReady:
			bt.logger.Println("SELECT- MetaReady at epoch", bt.epoch)
			// in master we check the completion of the epoch
			if bt.framework.GetTaskID() == 0 {
				_, exists := bt.fromOthers[mr.fromID]
				if exists {
					bt.logger.Panicf("Duplicated meta from task %d.", mr.fromID)
				}
				bt.logger.Printf("Master got metaReady from %d at epoch %d.", mr.fromID, bt.epoch)
				bt.fromOthers[mr.fromID] = true
				if len(bt.fromOthers) == int(bt.numOfTasks) {
					bt.logger.Printf("All neighbors have metaReady. Current epoch %d finished.", bt.epoch)
					bt.framework.IncEpoch(mr.ctx)
				}
			}
		case <-bt.exitChan:
			bt.logger.Println("SELECT- Exit at epoch", bt.epoch)
			return
		}
	}
}

func (bt *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	bt.logger.Println("EnterEpoch at task ", bt.taskID, " into ", epoch)
	bt.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (bt *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {

	bt.epoch = epoch
	bt.curIter++

	if bt.curIter > bt.numOfIters {
		// Job done. Dump results and finish it.
		// TODO(baigang) Save fullT and fullD to filesystem
	}

	// reset the data blocks
	bt.fullD = make(map[uint32]*pb.DenseMatrixShard)
	bt.fullT = make(map[uint32]*pb.DenseMatrixShard)
	bt.fromOthers = make(map[uint64]bool)

	//	bt.fromOthers[bt.taskID] = true

	var method string
	// In even epoch even, we fetch D and update T; in odd epoch, we fetch T and update D.
	if epoch%2 == 0 {
		method = "/proto.BlockData/GetDShard"
		// XXX: Add own block to it because neighbors doesn't include self.
		// bt.fullD[bt.blockId] = bt.shardedD
	} else {
		method = "/proto.BlockData/GetTShard"
		// bt.fullT[bt.blockId] = bt.shardedT
	}

	for _, c := range bt.framework.GetTopology().GetNeighbors("Neighbors", epoch) {
		bt.logger.Println("Sending request ", method, " to neighbor [", c, "] at epoch ", epoch)
		bt.framework.DataRequest(ctx, c, method, &pb.Request{Epoch: epoch}, bt.logger)
		bt.logger.Println("Done request ", method, " to neighbor [", c, "] at epoch ", epoch)
	}
	bt.logger.Println("Finished doEnterEpoch().")
}

// XXX(baigang): Master task will check this for invoking IncEpoch
func (bt *bwmfTask) MetaReady(ctx context.Context, fromID uint64, linkType, meta string) {
	bt.logger.Println("MetaReady  from task ", fromID)
	switch meta {
	case "metaReady":
		bt.logger.Println("Putting metaReady to bt.metaReady.")
		bt.metaReady <- &event{ctx: ctx, fromID: fromID}
	default:
		bt.logger.Panicf("Unknow meta: %s", meta)
	}
}

func (bt *bwmfTask) DataReady(ctx context.Context, fromID uint64, method string, output proto.Message) {
	bt.logger.Println("DataReady at task ", bt.taskID, " with method ", method, " from task ", fromID)
	switch method {
	case "/proto.BlockData/GetTShard":
		bt.dataReady <- &event{ctx: ctx, fromID: fromID, response: output}
	case "/proto.BlockData/GetDShard":
		bt.dataReady <- &event{ctx: ctx, fromID: fromID, response: output}
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
	bt.logWriter.Flush()
	// XXX Shall we dump the temporary results here or the last epoch?
}

// Implement of the BlockData service described in proto.
func (bt *bwmfTask) GetTShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// Here basically we send a getT request to the event channel. Then
	// the `Run()` will handle it.  The result will be sent back to channel
	// retT, and we fetch it here and respond it via grpc.
	retT := make(chan *pb.DenseMatrixShard, 1)
	bt.getT <- &event{ctx: ctx, epoch: request.Epoch, request: request, retT: retT}
	resT, ok := <-retT
	if !ok {
		return nil, fmt.Errorf("Failed getting T.")
	}
	close(retT)
	return &pb.Response{BlockId: bt.blockId, Shard: resT}, nil
}

func (bt *bwmfTask) GetDShard(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	// same as GetTShard
	retD := make(chan *pb.DenseMatrixShard, 1)
	bt.logger.Println("Requesting D at epoch ", request.Epoch)
	bt.getD <- &event{ctx: ctx, epoch: request.Epoch, request: request, retD: retD}
	resD, ok := <-retD
	if !ok {
		return nil, fmt.Errorf("Failed getting D.")
	}
	bt.logger.Println("Got responseD at epoch ", request.Epoch)
	close(retD)
	return &pb.Response{BlockId: bt.blockId, Shard: resD}, nil
}
