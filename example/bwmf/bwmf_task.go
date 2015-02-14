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
the epoch of iteration. "A full copy" consists of computation results from all slaves.
*/

// bwmfData is used to carry indexes and values associated with each index. Index here
// can be row or column id, and value can be K wide, one for each topic;
type bwmfData struct {
	Index  int
	Values []float64
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
	framework     taskgraph.Framework
	epoch, taskID uint64
	logger        *log.Logger
	numOfTasks    uint64
	NodeProducer  chan bool
	config        map[string]string

	// The original data.
	rowShard, columnShard []sparseVec

	dShard, tShard *bwmfData
	d, t           *bwmfData

	dtReady *common.CountDownLatch // We need to get all d or t ready before we start the local fitting.
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard() {}
func (t *bwmfTask) updateTShard() {}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}
func (t *bwmfTask) Exit() {}

func (t *bwmfTask) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {}

func (t *bwmfTask) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	if t.taskID != 0 {
		panic("only task 0 can have children")
	}

	// we need a map?
	t.dtReady.CountDown()
	// if we have all data, start next iteration.
	ctx.IncEpoch()
}

func (t *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	t.epoch = epoch

	// At epoch 0 (initialization):
	//   a. Read both rowShards and columnShards from disk.
	//   b. Random initialization of both dShard and tShard.
	// Then Task 0 will start the iterations -- we use epoch 1 here.
	//
	// After epoch 0:
	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.

	if epoch == 0 {
		// initialization
		return
	}

	if t.taskID == 0 {
		t.dtReady = common.NewCountDownLatch(1)
	}

	if t.epoch%2 == 0 {
		for index := uint64(1); index < t.numOfTasks; index++ {
			if index != t.taskID {
				ctx.DataRequest(index, "getD")
			}
		}
	} else {
		for index := uint64(1); index < t.numOfTasks; index++ {
			if index != t.taskID {
				ctx.DataRequest(index, "getT")
			}
		}
	}
}

func (t *bwmfTask) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {
	// Task zero should maintain the barrier here to get child from all the children,

	// This is the main body for the bwmf
	// There are three steps we need to handle:
	// Step A: we keep on collect all the D (or T) depending on which epoch we are on.

	// Step B: When we have all the data needed, we compute the local update for D (or T).

	// Step C: we notify master that we are done.
}

func (t *bwmfTask) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {}

// These are payload rpc for application purpose.
func (t *bwmfTask) ServeAsParent(fromID uint64, req string) []byte {
	if t.epoch%2 == 0 {
		b, err := json.Marshal(t.dShard)
		if err != nil {
			t.logger.Fatalf("Slave can't encode dShard error: %v\n", err)
		}
		return b
	} else {
		b, err := json.Marshal(t.tShard)
		if err != nil {
			t.logger.Fatalf("Slave can't encode tShard error: %v\n", err)
		}
		return b
	}
}

func (t *bwmfTask) ServeAsChild(fromID uint64, req string) []byte {
	return nil
}

type BWMFTaskBuilder struct {
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{}
}
