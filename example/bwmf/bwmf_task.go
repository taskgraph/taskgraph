package bwmf

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/op"
	"github.com/taskgraph/taskgraph/pkg/common"
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

	dtReady    *common.CountdownLatch
	fromOthers map[uint64]bool

	// The original data.
	namenodeAddr, webHdfsAddr, hdfsUser string
	rowShardPath, columnShardPath       string
	rowShard, columnShard               *MatrixShard

	shardedD, shardedT *MatrixShard
	fullD, fullT       []*MatrixShard

	// shardedD is size of m*k, t shard is size of k*n (but still its layed-out as n*k)
	// fullD is size of M*k, fullT is size of k*N (dittu, layout N*k)
	m, n, k, M, N uint32

	// parameters for projected gradient methods
	sigma, alpha, beta, tol float32

	// Parameter data. They actually shares the underlying storage (buffer) with shardedD/T and fullD/T.
	shardedDParam, shardedTParam, dParam, tParam taskgraph_op.Parameter

	// objective function, parameters and minimizer to solve bwmf
	tLoss, dLoss *KLDivLoss
	optimizer    *taskgraph_op.ProjectedGradient
	stopCriteria taskgraph_op.StopCriteria
}

// These two function carry out actual optimization.
func (bt *bwmfTask) updateDShard() {

	ok := bt.optimizer.Minimize(bt.dLoss, bt.stopCriteria, bt.shardedDParam)
	if !ok {
		// TODO report error
	}

}

func (bt *bwmfTask) updateTShard() {

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

	hdfsClient, hcOk := filesystem.NewHdfsClient(bt.namenodeAddr, bt.webHdfsAddr, bt.hdfsUser)
	if !hcOk {
		bt.logger.Fatal("Failed connecting to HDFS at ", bt.namenodeAddr, bt.webHdfsAddr, ", with account ", bt.hdfsUser)
	}

	var rowShardedOk, columnShardOk bool

	bt.rowShard, rowShardedOk = loadShardedMatrix("TODO/some/path/for/rowShard")
	bt.columnShard, columnShardOk = loadShardedMatrix("TODO/some/path/for/columnShard")

	if !rowShardedOk || !columnShardOk {
		bt.logger.Fatalf("Failed load matrix data on task: %d", bt.taskID)
	}

	// XXX(baigang) We set M and N via collecting all sharded D and T.
	// XXX(baigang) K is set in the task builder.
	bt.m = len(*bt.rowShard)
	bt.n = len(*bt.columnShard)

	// TODO we also need start indices for sharded D and T.
	bt.shardedD = &pb.MatrixShard{
		Rows: make([]pb.Row, bt.m),
	}
	bt.shardedT = &pb.MatrixShard{
		Rows: make([]pb.Row, bt.n),
	}
	for i, _ := range bt.shardedD.Rows {
		bt.shardedD.Rows[i].Row = make(map[uint32]float32)
	}
	for i, _ := range bt.shardedT.Rows {
		bt.shardedT.Rows[i].Row = make(map[uint32]float32)
	}

	bt.shardedDParam = taskgraph_op.NewVecParameter(bt.m * bt.k)
	bt.shardedTParam = taskgraph_op.NewVecParameter(bt.n * bt.k)
	bt.dParam = taskgraph_op.NewVecParameter(bt.M * bt.k)
	bt.tParam = taskgraph_op.NewVecParameter(bt.N * bt.k)

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

	bt.stopCriteria = taskgraph_op.MakeFixCountStopCriteria(bt.numOfIters)
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

	// Now all has been initalized.

	// At initialization:
	// Task 0 will start the iterations.
}

func (bt *bwmfTask) Exit() {
	// Shall we dump the temporary results here?
}

func (bt *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	bt.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", bt.taskID, epoch)
	bt.epoch = epoch
	bt.childrenReady = make(map[uint64]bool)
	bt.dtReady = common.NewCountdownLatch(int(bt.numOfTasks))

	// Afterwards:
	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
	// Even epochs: Fix D, calculate T;
	// Odd epochs: Fix T, calculate D;

	// XXX(baigang): This is for requesting sharded D/T from all tasks
	if bt.epoch%2 == 0 {
		for index := uint64(0); index < bt.numOfTasks; index++ {
			ctx.DataRequest(index, "getD")
		}
	} else {
		for index := uint64(0); index < bt.numOfTasks; index++ {
			ctx.DataRequest(index, "getT")
		}
	}

	go func() {
		// Wait for all shards (either D or T, depending on the epoch) to be ready.
		bt.dtReady.Await()

		// We can compute local shard result from A and D/T.
		if bt.epoch%2 == 0 {
			bt.updateTShard()
		} else {
			bt.updateDShard()
		}

		// Notify task 0 about the result.
		ctx.FlagMetaToParent("computed")
	}()
}
