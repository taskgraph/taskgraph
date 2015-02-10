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
There will be two kinds of Tasks: master and slaves.

We will have one master serve as the hub of star topology, its main objective is to set up the barrier
so that all the slave can wait until it is ok to proceed into next iteration (epoch).

We will have K slave that handles both row task and column task in alternation. Each slave will read
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

// bwmfMaster is mainly used for setting up barrier.
// Note: in theory, since there should be no parent of this, so we should
// add error checking in the right places. We will skip these test for now.
type bwmfMaster struct {
	dataChan           chan int32
	NodeProducer       chan bool
	framework          taskgraph.Framework
	epoch, taskID      uint64
	logger             *log.Logger
	config             map[string]string
	numberOfIterations uint64

	fromChildren map[uint64]byte
}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfMaster) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t *bwmfMaster) Exit() {}

// Ideally, we should also have the following:
func (t *bwmfMaster) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {
	t.logger.Fatal("We should not receive parent meta for master")
}

// We mainly need to make sure that we have all children coming back, it then call

func (t *bwmfMaster) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	t.logger.Printf("master ChildMetaReady, task: %d, epoch: %d, child: %d\n", t.taskID, t.epoch, childID)
	// Get data from child. When all the data is back, starts the next epoch.
	t.fromChildren[childID] = byte(1)
	// if we get child meta from all slave, we need to call framework to get into next epoch.
	// unless we got to the desired number of iterations then we quit.
}

// This give the task an opportunity to cleanup and regroup.
func (t *bwmfMaster) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("master SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)

	t.epoch = epoch

	// Make sure we have a clean slate.
	t.fromChildren = make(map[uint64]byte)
}

// These are payload rpc for application purpose.
func (t *bwmfMaster) ServeAsParent(fromID uint64, req string) []byte {
	t.logger.Fatal("We should not serve any data as parent on master")
	return nil
}

func (t *bwmfMaster) ServeAsChild(fromID uint64, req string) []byte {
	t.logger.Fatal("We should not serve any data as child on master")
	return nil
}

func (t *bwmfMaster) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {
	t.logger.Fatal("We should not receive any data as child on master")
}
func (t *bwmfMaster) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
	t.logger.Fatal("We should not receive any data as child on slave")
}

// bwmfSlave holds two shards of original matrixes (row and column shards), one shard of D, and one shard
// of T. It does thing different for odd epoach and even epoch. During odd epoch, it fetch all T from all
// other slaves, and finding better value for local shard of D, after it is done, it lets master know.
// During even epoch, it fetch all D from all other slaves, and finding better value for local shard of T.
type bwmfSlave struct {
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

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfSlave) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	// t.logger = log.New(ioutil.Discard, "", log.Ldate|log.Ltime|log.Lshortfile)
}

// Task need to finish up for exit, last chance to save work?
func (t *bwmfSlave) Exit() {}

// Ideally, we should also have the following:
func (t *bwmfSlave) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {
	t.logger.Fatal("We should not receive parent meta for master")
}

func (t *bwmfSlave) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	t.logger.Fatal("We should not receive child meta for master")
}

// This give the task an opportunity to cleanup and regroup.
//
func (t *bwmfSlave) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)

	t.dtReady = newCountDownLatch(1)

	t.epoch = epoch

	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
	meta := "GetDT"
	for index := uint64(1); index < t.numOfTasks; index++ {
		if index != t.taskID {
			ctx.DataRequest(index, meta)
		}
	}
}

// These are payload rpc for application purpose.
func (t *bwmfSlave) ServeAsParent(fromID uint64, req string) []byte {
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

func (t *bwmfSlave) ServeAsChild(fromID uint64, req string) []byte {
	return nil
}

func (t *bwmfSlave) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {
	// This is the main body for the bwmf
	// There are three steps we need to handle:
	// Step A: we keep on collect all the D (or T) depending on which epoch we are on.

	// Step B: When we have all the data needed, we compute the local update for D (or T).

	// Step C: we notify master that we are done.
}

func (t *bwmfSlave) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
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
	if taskID == 0 {
		return &bwmfMaster{
			dataChan:           tc.GDataChan,
			NodeProducer:       tc.NodeProducer,
			config:             tc.MasterConfig,
			numberOfIterations: tc.NumberOfIterations,
		}
	}
	return &bwmfSlave{
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
