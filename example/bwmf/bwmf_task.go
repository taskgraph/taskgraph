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
	sigma, alpha, beta    float64
	maxIterInSubProblem   int
	maxIterInFindingAlpha int
}

// These two function carry out actual optimization.
func (t *bwmfTask) updateDShard() {
	var newDShard Shard = t.dShard
	var ok bool
	for iter := 0; iter < t.maxIterInSubProblem; iter++ {
		newDShard, ok = t.solveSubproblem(&t.rowShard, &newDShard, &t.t)
		// TODO(baigang): dealing with ok==false
		// TODO(baigang): logging
	}
	t.dShard = newDShard
}
func (t *bwmfTask) updateTShard() {
	var newTShard Shard = t.tShard
	var ok bool
	for iter := 0; iter < t.maxIterInSubProblem; iter++ {
		newTShard, ok = t.solveSubproblem(&t.columnShard, &t.tShard, &t.d)
		// TODO(baigang): dealing with ok==false
		// TODO(baigang): logging
	}
	t.tShard = newTShard
}

// calculates $matrix^T * matrix$
func matrixSelfTransMult(matrix *Shard) Shard {
	var result Shard
	// TODO(baigang): implementation
	// length := len(matrix[0])
	return result
}

// calculates $a * b$ where $b$ is a sparseVec
func matrixSparseMult(a *Shard, b *[]sparseVec) Shard {
	var result Shard
	// TODO(baigang): implementation
	return result
}

// caculates $a * b$
func matrixMult(a, b *Shard) Shard {
	var result Shard
	// TODO(baigang): implementation
	return result
}

// caculates $a - b$
func matrixSub(a, b *Shard) Shard {
	var result Shard
	// TODO(baigang): implementation
	return result
}

// returns true is a equals b, as \|a - \|b_F^2 < epsilon
func matrixEqual(a, b *Shard) bool {
	// TODO(baigang): implementation
	return true
}

// calculates the sum of the component-wise product of matrx $\mathbf{a}$ and $\mathbf{b}$
func calcInProduct(a, b *Shard) float64 {
	// TODO(baigang): implementation
	return 1.0
}

// for each element in matrix $\mathbf{t} - alpha * \mathbf{g}$, do
//    elem = elem if elem > l && elem < u,
//           l  if elem <= l,
//           u  if elem >= u
func doProjection(t, g *Shard, alpha, l, u float64) Shard {
	result := make([]*bwmfData, len(*t))
	length := len((*t)[0].Values)
	for i, row := range *t {
		result[i].Values = make([]float64, length)
		for j, tValue := range (*t)[i].Values {
			gValue := alpha * (*g)[i].Values[j]
			value := tValue - gValue
			if value <= l {
				result[i].Values[j] = l
			} else if value >= u {
				result[i].Values[j] = u
			}
		}
	}
	return result
}

// Solve the problem: $argmin_t \| a - D*t \|_F^2$
// where $a$ is a column shard of $A$, $D$ is a full copy of factor D, and
// $t$ is a shard of factor T corresponding to $a$.
//
// XXX(baigang): This is currently an awkward translation of Matlab code listed in Appendix B.2 in ``Projected
// Gradient Methods for Non-negative Matrix Factorization'' with a renaming convention that V is A, W is D and
// H is tShard. Note this deals only the inner iter.
// TODO(baigang): stopping criteria of the outer iterations. Maybe also return norm of the gradient, i.e
// `projgrad` in B.2.
// TODO(baigang): Fine abstraction of shard data and matrix operations.
//
// A is an l-sized shard of the full matrix A, so its size is m*l, D is m*k, t is k*l.
func (t *bwmfTask) solveSubproblem(A *[]sparseVec, D, tShard *Shard) (Shard, bool) {
	DTD := matrixSelfTransMult(D)
	DTDt := matrixMult(&DTD, tShard)
	DTa := matrixSparseMult(D, A)

	gradient := matrixSub(&DTDt, &DTa)
	alpha := t.alpha
	decreaseAlpha := false
	// Step 2 in Algorithm 4, section 3.
	for iter := 0; iter <= t.maxIterInFindingAlpha; iter++ {
		tShardNew := doProjection(tShard, &gradient, t.alpha, 0.0, 1e20)
		tDiff := matrixSub(&tShardNew, tShard)
		DTDd := matrixMult(&DTD, &tDiff)

		// formula (17)
		conditionValue := (1.0-t.sigma)*calcInProduct(&gradient, &tDiff) + 0.5*calcInProduct(&tDiff, &DTDd)

		sufficientDecrese := conditionValue <= 0.0
		if iter == 0 {
			decreaseAlpha = !sufficientDecrese
		}
		// basically substep (b) in step 2, algorithm 4
		if decreaseAlpha {
			if sufficientDecrese {
				return tShardNew, true
			} else {
				alpha = alpha * t.beta
			}
		} else {
			if !sufficientDecrese || matrixEqual(&tShardNew, tShard) {
				return tShardNew, false
			} else {
				alpha = alpha / t.beta
				*tShard = tShardNew
			}
		}
	}
	return *tShard, false
}

// Initialization: We need to read row and column shards of A.
func (t *bwmfTask) readShardsFromDisk() {}

// Read dShard and tShard from last checkpoint if any.
func (t *bwmfTask) readLastCheckpoint() bool {
	panic("")
}

// Task have all the data, compute local optimization of D/T.
func (t *bwmfTask) localCompute() {}

// This is useful to bring the task up to speed from scratch or if it recovers.
func (t *bwmfTask) Init(taskID uint64, framework taskgraph.Framework) {
	t.taskID = taskID
	t.framework = framework
	t.logger = log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	// Use some unique identifier to set Index "in the future".
	// We can use taskID now.
	t.dShard = newDShard(t.taskID)
	t.tShard = newTShard(t.taskID)
	t.readShardsFromDisk()
	ok := t.readLastCheckpoint()
	if !ok {
		t.dShard.randomFillValue()
		t.tShard.randomFillValue()
	}

	// At initialization:
	// Task 0 will start the iterations.
}

func (t *bwmfTask) Exit() {}

func (t *bwmfTask) SetEpoch(ctx taskgraph.Context, epoch uint64) {
	t.logger.Printf("slave SetEpoch, task: %d, epoch: %d\n", t.taskID, epoch)
	t.epoch = epoch
	t.childrenReady = make(map[uint64]bool)
	t.dtReady = common.NewCountdownLatch(int(t.numOfTasks))

	// Afterwards:
	// We need to get all D/T from last epoch so that we can carry out local
	// update on T/D.
	// Even epochs: Fix D, calculate T;
	// Odd epochs: Fix T, calculate D;

	if t.epoch%2 == 0 {
		for index := uint64(0); index < t.numOfTasks; index++ {
			ctx.DataRequest(index, "getD")
		}
	} else {
		for index := uint64(0); index < t.numOfTasks; index++ {
			ctx.DataRequest(index, "getT")
		}
	}

	go func() {
		// Wait for all shards (either D or T, depending on the epoch) to be ready.
		t.dtReady.Await()
		// We can compute local shard result from A and D/T.
		t.localCompute()
		// Notify task 0 about the result.
		ctx.FlagMetaToParent("computed")
	}()
}

func (t *bwmfTask) ParentMetaReady(ctx taskgraph.Context, parentID uint64, meta string) {}

func (t *bwmfTask) ParentDataReady(ctx taskgraph.Context, parentID uint64, req string, resp []byte) {}

func (t *bwmfTask) ChildMetaReady(ctx taskgraph.Context, childID uint64, meta string) {
	// Task zero should maintain the barrier for iterations.
	if meta == "computed" {
		t.childrenReady[childID] = true
	}
	if uint64(len(t.childrenReady)) < t.numOfTasks {
		return
	}
	// if we have all data, start next iteration.
	ctx.IncEpoch()
}

// Other nodes has served with their local shards.
func (t *bwmfTask) ChildDataReady(ctx taskgraph.Context, childID uint64, req string, resp []byte) {
	t.dtReady.CountDown()
}

// get request of D/T shards from others. Serve with local shard.
func (t *bwmfTask) ServeAsParent(fromID uint64, req string, dataReceiver chan<- []byte) {
	var b []byte
	var err error

	go func() {
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
	}()
}

func (t *bwmfTask) ServeAsChild(fromID uint64, req string, dataReceiver chan<- []byte) {}

type BWMFTaskBuilder struct {
}

func (tb BWMFTaskBuilder) GetTask(taskID uint64) taskgraph.Task {
	return &bwmfTask{}
}
