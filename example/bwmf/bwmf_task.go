package bwmf

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	"github.com/taskgraph/taskgraph"
)

/*
The block wise matrix factorization task is designed for carry out block wise matrix factorization
for a variety of criteria (loss function) and constraints (nonnegativity for example).

The main idea behind the bwmf is following:
We will have K bwmfTasks that handles both row task and column task in alternation. Each bwmfTask will read
two copies of the data (one sharded by row, another sharded by column), it also host one shard of D and one
shard of T. Depending on which iteration it is in, it also have to potentially have a full copy of T or D
which it drains from all other slaves before it start its local interation.
*/

// bwmfData is used to carry indexes and values associated with each index. Index here
// can be row ro column id, and value can be K wide, one for each topic;
type bwmfData struct {
	Indexes []int32
	Values  []float32
}

type sparseVec struct {
	Index []int32
	Value []float32
}

// bwmfTasks holds two shards of original matrixes (row and column shards), one shard of D, and one shard
// of T. It does thing different for odd epoach and even epoch. During odd epoch, it fetch all T from all
// other slaves, and finding better value for local shard of D, after it is done, it let every one knows.
// Task 0 will monitor the progress and call framework.SetEpoch to start the new epoch.
// During even epoch, it fetch all D from all other slaves, and finding better value for local shard of T.
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

	dtReady *countDownLatch // We need to get all d or t ready before we start the local fitting.
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard() {}
func (t *bwmfTask) updateTShard() {}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	// t.logger = log.New(ioutil.Discard, "", log.Ldate|log.Ltime|log.Lshortfile)

	// Things need to be done here:
	// a. Read both rowShards and columnShards from disk.
	// b. Random initialized both dShard tShard.

	// These two are all we need to bootstrap the entire process.

	// finally task 0 start the job.
	if t.taskID == 0 {
		// t.framework.SetEpoch(0)
	}
}

// Task need to finish up for exit, last chance to save work?
func (t *bwmfTask) Exit() {}

// Ideally, we should also have the following:
func (t *bwmfTask) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {
	t.logger.Fatal("We should not receive parent meta for master")
}

func (t *bwmfTask) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	// Task zero should maintain the barrier here so that when it has ChildMeta from all the children,
	// it will call framework.SetEpoch to increment the epoch.
	if t.taskID == 0 {

	}
}

// This give the task an opportunity to cleanup and regroup.
//
func (t *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)

	t.dtReady = newCountDownLatch(1)

	t.epoch = epoch

	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
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

func (t *bwmfTask) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {
	// This is the main body for the bwmf
	// There are three steps we need to handle:
	// Step A: we keep on collect all the D (or T) depending on which epoch we are on.

	// Step B: When we have all the data needed, we compute the local update for D (or T).

	// Step C: we notify master that we are done.
}

func (t *bwmfTask) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
	t.logger.Fatal("We should not receive child meta for master")
}

// used for testing
type SimpleTaskBuilder struct {
	GDataChan          chan int32
	NumberOfIterations uint64
	NodeProducer       chan bool
	MasterConfig       map[string]string
	SlaveConfig        map[string]string
}

// This method is called once by framework implementation to get the
// right task implementation for the node/task. It requires the taskID
// for current node, and also a global array of tasks.
func (tc SimpleTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{
		NodeProducer: tc.NodeProducer,
		config:       tc.SlaveConfig,
	}
}

// I am writing this count down latch because sync.WaitGroup doesn't support
// decrementing counter when it's 0.
type countDownLatch struct {
	sync.Mutex
	cond    *sync.Cond
	counter int
}

func newCountDownLatch(count int) *countDownLatch {
	c := new(countDownLatch)
	c.cond = sync.NewCond(c)
	c.counter = count
	return c
}

func (c *countDownLatch) Count() int {
	c.Lock()
	defer c.Unlock()
	return c.counter
}

func (c *countDownLatch) CountDown() {
	c.Lock()
	defer c.Unlock()
	if c.counter == 0 {
		return
	}
	c.counter--
	if c.counter == 0 {
		c.cond.Broadcast()
	}
}

func (c *countDownLatch) Await() {
	c.Lock()
	defer c.Unlock()
	if c.counter == 0 {
		return
	}
	c.cond.Wait()
}
