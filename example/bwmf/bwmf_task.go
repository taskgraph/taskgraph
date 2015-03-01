package bwmf

import (
	"encoding/json"
	"log"
	"os"

	"github.com/taskgraph/taskgraph"
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

// bwmfData is used to carry indexes and values associated with each index. Index here
// can be row or column id, and value can be K wide, one for each topic;
type bwmfData struct {
	Index  int
	Values []float64
}

type Shard []*bwmfData

func newDShard(index uint64) []*bwmfData {
	panic("")
}
func newTShard(index uint64) []*bwmfData {
	panic("")
}

func (shard *Shard) randomFillValue() {
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

	dtReady       *common.CountdownLatch
	childrenReady map[uint64]bool

	dShard, tShard Shard
	d, t           Shard

	// parameters for projected gradient methods
	sigma, alpha, beta float64

	// objective function, parameters and minimizer to solve bwmf
	// TODO
}

// These two function carry out actual optimization.
func (this *bwmfTask) updateDShard() {
}

func (this *bwmfTask) updateTShard() {
}

// Initialization: We need to read row and column shards of A.
func (this *bwmfTask) readShardsFromDisk() {}

// Read dShard and tShard from last checkpoint if any.
func (this *bwmfTask) readLastCheckpoint() bool {
	panic("")
}

// Task have all the data, compute local optimization of D/T.
func (this *bwmfTask) localCompute() {

}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (this *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	this.taskID = taskID
	this.framework = framework
	this.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	// Use some unique identifier to set Index "in the future".
	// We can use taskID now.
	this.dShard = newDShard(this.taskID)
	this.tShard = newTShard(this.taskID)
	this.readShardsFromDisk()
	ok := this.readLastCheckpoint()
	if !ok {
		this.dShard.randomFillValue()
		this.tShard.randomFillValue()
	}

	// At initialization:
	// Task 0 will start the iterations.
}

func (this *bwmfTask) Exit() {}

func (this *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	this.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", this.taskID, epoch)
	this.epoch = epoch
	this.childrenReady = make(map[uint64]bool)
	this.dtReady = common.NewCountdownLatch(int(this.numOfTasks))

	// Afterwards:
	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
	// Even epochs: Fix D, calculate T;
	// Odd epochs: Fix T, calculate D;

	if this.epoch%2 == 0 {
		for index := uint64(0); index < this.numOfTasks; index++ {
			ctx.DataRequest(index, "getD")
		}
	} else {
		for index := uint64(0); index < this.numOfTasks; index++ {
			ctx.DataRequest(index, "getT")
		}
	}

	go func() {
		// Wait for all shards (either D or T, depending on the epoch) to be ready.
		this.dtReady.Await()
		// We can compute local shard result from A and D/T.
		this.localCompute()
		// Notify task 0 about the result.
		ctx.FlagMetaToParent("computed")
	}()
}

func (this *bwmfTask) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {}

func (this *bwmfTask) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {
}

func (this *bwmfTask) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	// Task zero should maintain the barrier for iterations.
	if meta == "computed" {
		this.childrenReady[childID] = true
	}
	if uint64(len(this.childrenReady)) < this.numOfTasks {
		return
	}
	// if we have all data, start next iteration.
	ctx.IncEpoch()
}

// Other nodes has served with their local shards.
func (this *bwmfTask) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
	this.dtReady.CountDown()
}

// get request of D/T shards from others. Serve with local shard.
func (this *bwmfTask) ServeAsParent(fromID uint64, req string, dataReceiver chan<- []byte) {
	var b []byte
	var err error

	go func() {
		if this.epoch%2 == 0 {
			b, err = json.Marshal(this.dShard)
			if err != nil {
				this.logger.Fatalf("Slave can't encode dShard error: %v\n", err)
			}
		} else {
			b, err = json.Marshal(this.tShard)
			if err != nil {
				this.logger.Fatalf("Slave can't encode tShard error: %v\n", err)
			}
		}
		dataReceiver <- b
	}()
}

func (this *bwmfTask) ServeAsChild(fromID uint64, req string, dataReceiver chan<- []byte) {}

type BWMFTaskBuilder struct {
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{}
}
