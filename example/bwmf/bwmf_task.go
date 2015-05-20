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
	curIter    uint64

	rowShard    *pb.MatrixShard
	columnShard *pb.MatrixShard
	dShard      *pb.MatrixShard
	tShard      *pb.MatrixShard

	peerShards  map[uint64]*pb.MatrixShard
	peerUpdated map[uint64]bool

	dims   *dimensions
	config *Config

	fsClient filesystem.Client

	// optimization toolkits
	dLoss        *KLDivLoss
	tLoss        *KLDivLoss
	dParam       op.Parameter
	tParam       op.Parameter
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
	m, n, k uint32
	M, N    uint32
}

func (t *bwmfTask) initData() {
	var rsErr, csErr error

	rowShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.IDPath, t.taskID)
	columnShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.ITPath, t.taskID)
	t.rowShard, rsErr = LoadMatrixShard(t.fsClient, rowShardPath)
	if rsErr != nil {
		t.logger.Panicf("Failed load rowShard from %s with error %s", rowShardPath, rsErr)
	}
	t.columnShard, csErr = LoadMatrixShard(t.fsClient, columnShardPath)
	if csErr != nil {
		t.logger.Panicf("Failed load columnShard from %s with error %s", columnShardPath, csErr)
	}

	t.dims = &dimensions{
		m: t.rowShard.M,
		n: t.columnShard.M,
		k: t.config.OptConf.DimLatent,
		M: 0,
		N: 0,
	}

	if t.config.IOConf.InitDPath != "" {
		t.dShard, rsErr = LoadMatrixShard(t.fsClient, t.config.IOConf.InitDPath+"."+strconv.Itoa(int(t.taskID)))
		if rsErr != nil {
			t.logger.Panicf("Failed initialize dShard. %s", rsErr)
		}
		if t.dShard.M != t.dims.m {
			t.logger.Panic("Dimension mismatch for pre-initialized dShard.")
		}
	}

	if t.config.IOConf.InitTPath != "" {
		t.tShard, rsErr = LoadMatrixShard(t.fsClient, t.config.IOConf.InitTPath+"."+strconv.Itoa(int(t.taskID)))
		if rsErr != nil {
			t.logger.Panicf("Failed initialize tShard. %s", rsErr)
		}
		if t.tShard.M != t.dims.n {
			t.logger.Panic("Dimension mismatch for pre-initialized tShard.")
		}
	}
}

func (t *bwmfTask) initOptUtil() {
	if t.tShard == nil {
		// XXX: Initialize it random and sparse.
		t.tShard = &pb.MatrixShard {
			M: t.dims.n,
			N: t.dims.k,
			Val: make([]float32, t.dims.n * t.dims.k),
		}
		for i := uint32(0); i < t.tShard.M * t.tShard.N; i++ {
			t.tShard.Val[i] = rand.Float32()
		}
	}

	if t.dShard == nil {
		// XXX: Initial at 0.0.
		t.dShard = &pb.MatrixShard {
			M: t.dims.m,
			N: t.dims.k,
			Val: make([]float32, t.dims.m * t.dims.k),
		}

		for i := uint32(0); i < t.dShard.M * t.dShard.N; i++ {
			t.dShard.Val[i] = rand.Float32()
		}
	}

	t.tParam = op.NewVecParameterWithData(t.tShard.Val)
	t.dParam = op.NewVecParameterWithData(t.dShard.Val)

	tParamLen := t.dims.n * t.dims.k
	dParamLen := t.dims.m * t.dims.k
	projLen := uint32(0)
	if tParamLen > dParamLen {
		projLen = tParamLen
	} else {
		projLen = dParamLen
	}

	t.optimizer = op.NewProjectedGradient(
		op.NewProjection(
			op.NewAllTheSameParameter(1e20, int(projLen)),
			op.NewAllTheSameParameter(1e-8, int(projLen)),
		),
		t.config.OptConf.Beta,
		t.config.OptConf.Sigma,
		t.config.OptConf.Alpha,
	)
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
	t.dataReady = make(chan *event, t.numOfTasks)
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
	columnShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.OTPath, t.taskID)
	rowShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.ODPath, t.taskID)
	err := SaveMatrixShard(t.fsClient, t.tShard, columnShardPath)
	if err != nil {
		t.logger.Printf("Save tShard for task %d failed with error: %v", t.taskID, err)
	}
	err = SaveMatrixShard(t.fsClient, t.dShard, rowShardPath)
	if err != nil {
		t.logger.Printf("Save dShard for task %d failed with error: %v", t.taskID, err)
	}

	t.logger.Println("Finished. Waiting for the framework to stop the task...")
}

func (t *bwmfTask) EnterEpoch(ctx context.Context, epoch uint64) {
	t.epochChange <- &event{ctx: ctx, epoch: epoch}
}

func (t *bwmfTask) doEnterEpoch(ctx context.Context, epoch uint64) {
	t.logger.Printf("doEnterEpoch, task %d, epoch %d", t.taskID, epoch)
	t.peerShards = make(map[uint64]*pb.MatrixShard)
	t.peerUpdated = make(map[uint64]bool)
	t.epoch = epoch
	t.stopCriteria = op.MakeComposedCriterion(
		op.MakeFixCountStopCriteria(t.config.OptConf.FixedCnt),
		op.MakeGradientNormStopCriteria(t.config.OptConf.GradTol),
		op.MakeTimeoutCriterion(300*time.Second),
	)

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
			if t.dims.M == 0 {
				t.dims.M = 0
				for _, m := range t.peerShards {
					t.dims.M += m.M
				}
			}
			go t.updateShard(ctx, t.dims.M, t.dims.n, t.dims.k, t.tParam, t.columnShard)
		} else {
			t.logger.Printf("Full T ready, task %d, epoch %d", t.taskID, t.epoch)
			// XXX Starting an intensive computation.
			if t.dims.N == 0 {
				t.dims.N = 0
				for _, m := range t.peerShards {
					t.dims.N += m.M
				}
			}
			go t.updateShard(ctx, t.dims.N, t.dims.m, t.dims.k, t.dParam, t.rowShard)
		}
	}
}

func (t *bwmfTask) updateShard(ctx context.Context, m, n, k uint32, H op.Parameter, V *pb.MatrixShard) {
	W := make([]*pb.MatrixShard, t.numOfTasks)
	for i, m := range t.peerShards {
		W[i] = m
	}

	loss := NewKLDivLoss(V, W, m, n, k, 1e-9)
	val, optErr := t.optimizer.Minimize(loss, t.stopCriteria, H)
	if optErr != nil {
		t.logger.Panicf("Failed minimizing over shard: %s", optErr)
		// handle re-run
		// XXX(baigang) just kill the framework and wait for restarting?
	}
	t.logger.Printf("Updated shard, loss is %f", val)
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
		if t.epoch < 2*t.config.OptConf.NumIters {
			t.framework.IncEpoch(ctx)
		} else {
			t.framework.ShutdownJob()
		}
	}
}
