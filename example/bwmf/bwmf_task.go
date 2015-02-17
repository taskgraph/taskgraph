package bwmf

import (
	"encoding/json"
	"log"
	"os"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/pkg/common"
)

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

// bwmfData is used to carry indexes and values associated with each index. Index here
// can be row or column id, and value can be K wide, one for each topic;
type bwmfData struct {
	Index  int
	Values []float64
}

func (d *bwmfData) randomFillValue() {

}

type sparseVec struct {
	Indexes []int
	Values  []float64
}

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

	// The original data.
	rowShard, columnShard []sparseVec

	dtReady        *common.CountdownLatch
	localCompleted *common.CountdownLatch
	childrenReady  map[uint64]bool

	dShard, tShard *bwmfData
	d, t           []*bwmfData
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard() {}
func (t *bwmfTask) updateTShard() {}

// Initialization: We need to read row and column shards of A.
func (t *bwmfTask) readShardsFromDisk() {}

// Read dShard and tShard from last checkpoint if any.
func (t *bwmfTask) readLastCheckpoint() {}

// Task have all the data, compute local optimization of D/T.
func (t *bwmfTask) localCompute() {}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	t.readShardsFromDisk()
	// How to set Index for d/tShard?
	t.readLastCheckpoint()
}
func (t *bwmfTask) Exit() {}

func (t *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	t.epoch = epoch
	t.childrenReady = make(map[uint64]bool)
	t.dtReady = common.NewCountdownLatch(int(t.numOfTasks - 1))
	t.localCompleted = common.NewCountdownLatch(1)

	// At epoch 0 (initialization):
	// Random initialization of both dShard and tShard.
	// Then Task 0 will start the iterations -- we use epoch 1 here.
	//
	// After epoch 0:
	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
	// Even epochs: Fix D, calculate T;
	// Odd epochs: Fix T, calculate D;

	if epoch == 0 {
		t.dShard.randomFillValue()
		t.tShard.randomFillValue()

		if t.taskID == 0 {
			ctx.IncEpoch()
		}
		return
	}

	if t.epoch%2 == 0 {
		for index := uint64(0); index < t.numOfTasks; index++ {
			if index != t.taskID {
				ctx.DataRequest(index, "getD")
			}
		}
	} else {
		for index := uint64(0); index < t.numOfTasks; index++ {
			if index != t.taskID {
				ctx.DataRequest(index, "getT")
			}
		}
	}

	go func() {
		t.dtReady.Await()
		t.localCompute()
		if t.taskID != 0 {
			ctx.FlagMetaToChild("computed")
		} else {
			t.localCompleted.CountDown()
		}
	}()
}

func (t *bwmfTask) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {}

func (t *bwmfTask) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {}

func (t *bwmfTask) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	// Task zero should maintain the barrier for iterations.
	if t.taskID != 0 {
		return
	}

	t.childrenReady[childID] = true
	if uint64(len(t.childrenReady)) < t.numOfTasks-1 {
		return
	}
	// if we have all data, start next iteration.
	go func() {
		t.localCompleted.Await()
		ctx.IncEpoch()
	}()
}

func (t *bwmfTask) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
	t.dtReady.CountDown()
}

// These are payload rpc for application purpose.
func (t *bwmfTask) ServeAsParent(fromID uint64, req string, dataReceiver chan<- []byte) {
	var b []byte
	var err error
	if t.epoch%2 == 0 {
		b, err = json.Marshal(t.dShard)
		if err != nil {
			t.logger.Fatalf("Slave can't encode dShard error: %v\n", err)
		}
	} else {
		b, err = json.Marshal(t.tShard)
		if err != nil {
			t.logger.Fatalf("Slave can't encode tShard error: %v\n", err)
		}
	}
	dataReceiver <- b
}

func (t *bwmfTask) ServeAsChild(fromID uint64, req string, dataReceiver chan<- []byte) {}

type BWMFTaskBuilder struct {
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{}
}
