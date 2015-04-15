package bwmf

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
	"github.com/taskgraph/taskgraph/op"
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
	numIters   uint64
	curIter    uint64

	rowShard    *pb.SparseMatrixShard
	columnShard *pb.SparseMatrixShard
	dShard      *pb.DenseMatrixShard
	tShard      *pb.DenseMatrixShard

	peerShards  map[uint64]*pb.DenseMatrixShard
	peerUpdated map[uint64]bool

	dims      *dimensions
	config    *Config
	latentDim int

	fsClient filesystem.Client

	// optimization toolkits
	dLoss        *KLDivLoss
	tLoss        *KLDivLoss
	optimizer    *op.ProjectedGradient
	stopCriteria op.StopCriteria

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

type dimensions struct {
	m, n, k int
	M, N    int
}

func (t *bwmfTask) initData() {

	var rsErr, csErr error

	t.rowShard, rsErr = LoadSparseShard(t.fsClient, t.config.IOConf.IDPath+"."+strconv.Itoa(int(t.taskID)))
	if rsErr != nil {
		t.logger.Panicf("Failed load rowShard. %s", rsErr)
	}
	t.columnShard, csErr = LoadSparseShard(t.fsClient, t.config.IOConf.ITPath+"."+strconv.Itoa(int(t.taskID)))
	if csErr != nil {
		t.logger.Panicf("Failed load columnShard. %s", csErr)
	}

	t.dims = &dimensions{
		m: len(t.rowShard.Row),
		n: len(t.columnShard.Row),
		k: t.latentDim,
		M: -1,
		N: -1,
	}

	t.dShard, _ = initDenseShard(t.dims.m, t.dims.k)
	t.tShard, _ = initDenseShard(t.dims.n, t.dims.k)
}

func (t *bwmfTask) initOptUtil() {
	// init optimization utils
	projLen := 0
	if t.dims.m > t.dims.n {
		projLen = t.dims.m * t.dims.k
	} else {
		projLen = t.dims.n * t.dims.k
	}

	t.optimizer = op.NewProjectedGradient(
		op.NewProjection(
			op.NewAllTheSameParameter(1e20, projLen),
			op.NewAllTheSameParameter(1e-8, projLen),
		),
		t.config.OptConf.Beta,
		t.config.OptConf.Sigma,
		t.config.OptConf.Alpha,
	)

	t.stopCriteria = op.MakeComposedCriterion(
		op.MakeFixCountStopCriteria(15),
		op.MakeGradientNormStopCriteria(t.config.OptConf.GradTol),
		op.MakeTimeoutCriterion(300*time.Second),
	)
}

func initDenseShard(l, k int) (*pb.DenseMatrixShard, error) {
	shard := &pb.DenseMatrixShard{
		Row: make([]*pb.DenseMatrixShard_DenseRow, l),
	}
	for i, _ := range shard.Row {
		shard.Row[i] = &pb.DenseMatrixShard_DenseRow{At: make([]float32, k)}
		for j, _ := range shard.Row[i].At {
			shard.Row[i].At[j] = rand.Float32()
		}
	}
	return shard, nil
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)

	t.initData()
	t.initOptUtil()

	t.epochChange = make(chan *event, 1)
	t.getT = make(chan *event, 1)
	t.getD = make(chan *event, 1)
	t.dataReady = make(chan *event, 1)
	t.updateDone = make(chan *event, 1)
	t.metaReady = make(chan *event, 1)
	t.exitChan = make(chan *event)
	go t.run()
}

func (t *bwmfTask) getDShard() *pb.Response {
	return &pb.Response{
		BlockId: t.taskID,
		Shard:   t.dShard,
	}
}

func (t *bwmfTask) getTShard() *pb.Response {
	return &pb.Response{
		BlockId: t.taskID,
		Shard:   t.tShard,
	}
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
	t.finish()
}

func (t *bwmfTask) finish() {
	// TODO save result to filesystem
	// but we can dump it to logger here for now
	t.logger.Println("tShard: ", *t.tShard)
	t.logger.Println("dShard: ", *t.dShard)
	t.logger.Println("Finished. Waiting for the framework to stop the task...")
}

func (t *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (t *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("doEnterEpoch, task %d, epoch %d", t.taskID, epoch)
	t.peerShards = make(map[uint64]*pb.DenseMatrixShard)
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
	resp, bOk := output.(*pb.Response)
	if !bOk {
		t.logger.Panicf("doDataRead, corruption in proto.Message.")
	}
	t.peerShards[resp.BlockId] = resp.Shard
	if len(t.peerShards) == int(t.numOfTasks) {
		if t.epoch%2 == 0 {
			t.logger.Printf("Full D ready, task %d, epoch %d", t.taskID, t.epoch)
			// XXX Starting an intensive computation.
			go t.updateTShard(ctx)
		} else {
			t.logger.Printf("Full T ready, task %d, epoch %d", t.taskID, t.epoch)
			// XXX Starting an intensive computation.
			go t.updateDShard(ctx)
		}
	}
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard(ctx context.Context) {
	if t.dims.N == -1 {
		t.dims.N = 0
		for _, m := range t.peerShards {
			t.dims.N += len(m.Row)
		}
		wh := make([][]float32, t.dims.N)
		for i, _ := range wh {
			wh[i] = make([]float32, t.dims.m)
		}
		t.dLoss = &KLDivLoss{
			V:  t.rowShard,
			WH: wh,
			m:  t.dims.N,
			n:  t.dims.m,
			k:  t.dims.k,
		}
	}

	t.dLoss.W = NewBlocksParameter(&t.peerShards)
	param := NewSingleBlockParameter(t.dShard)

	loss, optErr := t.optimizer.Minimize(t.dLoss, t.stopCriteria, param)
	if optErr != nil {
		t.logger.Panicf("Failed minimizing over dShard: %s", optErr)
		// handle re-run
		// XXX(baigang) just kill the framework and wait for restarting?
	}
	t.logger.Printf("Updated dShard, loss is %f", loss)

	t.updateDone <- &event{ctx: ctx}
}

func (t *bwmfTask) updateTShard(ctx context.Context) {
	if t.dims.M == -1 {
		t.dims.M = 0
		for _, m := range t.peerShards {
			t.dims.M += len(m.Row)
		}
		wh := make([][]float32, t.dims.M)
		for i, _ := range wh {
			wh[i] = make([]float32, t.dims.n)
		}
		t.tLoss = &KLDivLoss{
			V:  t.columnShard,
			WH: wh,
			m:  t.dims.M,
			n:  t.dims.n,
			k:  t.dims.k,
		}
	}

	t.tLoss.W = NewBlocksParameter(&t.peerShards)
	param := NewSingleBlockParameter(t.tShard)

	loss, optErr := t.optimizer.Minimize(t.tLoss, t.stopCriteria, param)
	if optErr != nil {
		t.logger.Panicf("Failed minimizing over tShard:", optErr)
		// TODO handle re-run
	}
	t.logger.Printf("Updated tShard, loss is %f", loss)

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
		if t.epoch < 2*t.numIters {
			t.framework.IncEpoch(ctx)
		} else {
			t.framework.ShutdownJob()
		}
	}
}
