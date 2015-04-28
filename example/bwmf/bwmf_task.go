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
	m, n, k int
	M, N    int
}

func (t *bwmfTask) initData() {
	var rsErr, csErr error

	rowShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.IDPath, t.taskID)
	columnShardPath := fmt.Sprintf("%s-%06d", t.config.IOConf.ITPath, t.taskID)
	t.rowShard, rsErr = LoadMatrixShard(t.fsClient, rowShardPath)
	if rsErr != nil {
		t.logger.Panicf("Failed load rowShard from %s with error %s", rsErr)
	}
	t.columnShard, csErr = LoadMatrixShard(t.fsClient, columnShardPath)
	if csErr != nil {
		t.logger.Panicf("Failed load columnShard from %s with error %s", csErr)
	}

	t.dims = &dimensions{
		m: len(t.rowShard.Row),
		n: len(t.columnShard.Row),
		k: t.config.OptConf.DimLatent,
		M: -1,
		N: -1,
	}

	if t.config.IOConf.InitDPath != "" {
		t.dShard, rsErr = LoadMatrixShard(t.fsClient, t.config.IOConf.InitDPath+"."+strconv.Itoa(int(t.taskID)))
		if rsErr != nil {
			t.logger.Panicf("Failed initialize dShard. %s", rsErr)
		}
		if len(t.dShard.Row) != t.dims.m {
			t.logger.Panic("Dimension mismatch for pre-initialized dShard.")
		}
	}

	if t.config.IOConf.InitTPath != "" {
		t.tShard, rsErr = LoadMatrixShard(t.fsClient, t.config.IOConf.InitTPath+"."+strconv.Itoa(int(t.taskID)))
		if rsErr != nil {
			t.logger.Panicf("Failed initialize tShard. %s", rsErr)
		}
		if len(t.tShard.Row) != t.dims.n {
			t.logger.Panic("Dimension mismatch for pre-initialized tShard.")
		}
	}
}

func (t *bwmfTask) initOptUtil() {
	// init optimization utils
	tParamLen := t.dims.n * t.dims.k
	dParamLen := t.dims.m * t.dims.k

	t.tParam = op.NewVecParameter(tParamLen)
	t.dParam = op.NewVecParameter(dParamLen)

	if t.tShard == nil {
		// XXX: Initialize it random and sparse.
		t.tShard = &pb.MatrixShard{
			Row: make([]*pb.MatrixShard_RowData, t.dims.n),
		}
		for r, _ := range t.tShard.Row {
			t.tShard.Row[r] = &pb.MatrixShard_RowData{RowId: int32(r), At: make(map[int32]float32)}
			for k := 0; k < t.dims.k; k++ {
				t.tShard.Row[r].At[int32(k)] = rand.Float32()
			}
		}
	}
	// assign loaded tshard to tparam
	for r, m := range t.tShard.GetRow() {
		for c, v := range m.At {
			t.tParam.Set(r*t.dims.k+int(c), v)
		}
	}

	if t.dShard == nil {
		// XXX: Initial at 0.0.
		t.dShard = &pb.MatrixShard{
			Row: make([]*pb.MatrixShard_RowData, t.dims.m),
		}

		for r, _ := range t.dShard.Row {
			t.dShard.Row[r] = &pb.MatrixShard_RowData{RowId: int32(r), At: make(map[int32]float32)}
			for k := 0; k < t.dims.k; k++ {
				t.dShard.Row[r].At[int32(k)] = rand.Float32()
			}
		}
	}
	// assign loaded dshard to dparam
	for r, m := range t.dShard.GetRow() {
		for c, v := range m.At {
			t.dParam.Set(r*t.dims.k+int(c), v)
		}
	}

	projLen := 0
	if tParamLen > dParamLen {
		projLen = tParamLen
	} else {
		projLen = dParamLen
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
			if t.dims.M == -1 {
				t.dims.M = 0
				for _, m := range t.peerShards {
					t.dims.M += len(m.Row)
				}
			}
			go t.updateShard(ctx, t.dims.M, t.dims.n, t.dims.k, t.tParam, t.tShard, t.columnShard)
		} else {
			t.logger.Printf("Full T ready, task %d, epoch %d", t.taskID, t.epoch)
			// XXX Starting an intensive computation.
			if t.dims.N == -1 {
				t.dims.N = 0
				for _, m := range t.peerShards {
					t.dims.N += len(m.Row)
				}
			}
			go t.updateShard(ctx, t.dims.N, t.dims.m, t.dims.k, t.dParam, t.dShard, t.rowShard)
		}
	}
}

func (t *bwmfTask) updateShard(ctx context.Context, m, n, k int,
	param op.Parameter, shard *pb.MatrixShard, full *pb.MatrixShard) {
	W := make([]*pb.MatrixShard, t.numOfTasks)
	for i, m := range t.peerShards {
		W[i] = m
	}

	loss := NewKLDivLoss(full, W, m, n, k, 1e-9)
	val, optErr := t.optimizer.Minimize(loss, t.stopCriteria, param)
	if optErr != nil {
		t.logger.Panicf("Failed minimizing over shard: %s", optErr)
		// handle re-run
		// XXX(baigang) just kill the framework and wait for restarting?
	}

	copyParamToShard(param, shard, k)
	t.logger.Printf("Updated shard, loss is %f", val)
	t.updateDone <- &event{ctx: ctx}
}

func copyParamToShard(param op.Parameter, shard *pb.MatrixShard, k int) {
	for iter := param.IndexIterator(); iter.Next(); {
		index := iter.Index()
		r := index / k
		c := index % k
		v := param.Get(index)
		if v > 1e-6 {
			shard.Row[r].At[int32(c)] = v
		} else {
			delete(shard.Row[r].At, int32(c))
		}
	}
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
