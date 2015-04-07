package bwmf

import (
	"fmt"
	"net"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"

	"github.com/taskgraph/taskgraph"
	"github.com/taskgraph/taskgraph/controller"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/framework"
)

func TestBWMF(t *testing.T) {
	etcdURLs := []string{"http://localhost:4001"}

	job := "bwmf_integration_test"
	numOfTasks := uint64(2)
	numOfIterations := uint64(10)

	// controller to set up directories in etcd
	controller := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks, []string{"Neighbors", "Master"})
	controller.Start()

	r1, c1, r2, c2, err := getBufs()
	if err != nil {
		panic("Failed generating data.")
	}
	taskBuilder_0 := &BWMFTaskBuilder{
		NumOfTasks:     numOfTasks,
		NumOfIters:     numOfIterations,
		PgmSigma:       0.1,
		PgmAlpha:       0.5,
		PgmBeta:        0.1,
		PgmTol:         1e-4,
		BlockId:        0,
		K:              2,
		RowShardBuf:    r1,
		ColumnShardBuf: c1,
		WorkPath:       "./",
	}
	taskBuilder_1 := &BWMFTaskBuilder{
		NumOfTasks:     numOfTasks,
		NumOfIters:     numOfIterations,
		PgmSigma:       0.1,
		PgmAlpha:       0.5,
		PgmBeta:        0.1,
		PgmTol:         1e-4,
		BlockId:        1,
		K:              2,
		RowShardBuf:    r2,
		ColumnShardBuf: c2,
		WorkPath:       "./",
	}

	// start the tasks
	go drive(t, job, etcdURLs, numOfTasks, taskBuilder_0)
	go drive(t, job, etcdURLs, numOfTasks, taskBuilder_1)

	// TODO check it here

	// clear up
	controller.WaitForJobDone()
	controller.Stop()
}

func createListener(t *testing.T) net.Listener {
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(\"tcp4\", \"\") failed: %v", err)
	}
	return l
}

// This is used to show how to drive the network.
func drive(t *testing.T, jobName string, etcds []string, ntask uint64, taskBuilder taskgraph.TaskBuilder) {
	bootstrap := framework.NewBootStrap(jobName, etcds, createListener(t), nil)
	bootstrap.SetTaskBuilder(taskBuilder)
	bootstrap.SetTopology(topo.NewFullTopology(ntask))
	bootstrap.Start()
}

//// Generating artificial test data

// The ground-truth D is
//   --------------
//   | 0.7  | 0.3 |
//   | 1.0  | 0.0 |
//   | 0.0  | 1.0 |
//   --------------
//
// The ground-truth T is
//  -------------------------
//  | 0.3 | 0.0 | 0.1 | 1.0 |
//  | 0.7 | 1.0 | 0.9 | 0.0 |
//  -------------------------
//
// So the whole matrix A is
//  -----------------------------
//  | 0.42 | 0.30 | 0.34 | 0.70 |
//  | 0.30 | 0.00 | 0.10 | 1.00 |
//  | 0.70 | 1.00 | 0.90 | 0.00 |
//  -----------------------------
//
func getBufs() (row1, column1, row2, column2 []byte, err error) {

	// shard1: row 0,1 of A
	rowShard1 := &pb.SparseMatrixShard{
		Row: []*pb.SparseMatrixShard_SparseRow{&pb.SparseMatrixShard_SparseRow{}, &pb.SparseMatrixShard_SparseRow{}},
	}
	rowShard1.Row[0].At = make(map[int32]float32)
	rowShard1.Row[1].At = make(map[int32]float32)

	rowShard1.Row[0].At[0] = 0.42
	rowShard1.Row[0].At[1] = 0.30
	rowShard1.Row[0].At[2] = 0.34
	rowShard1.Row[0].At[3] = 0.70

	rowShard1.Row[1].At[0] = 0.30
	rowShard1.Row[1].At[2] = 0.10
	rowShard1.Row[1].At[3] = 1.00

	bufRow1, r1Err := proto.Marshal(rowShard1)

	// shard1: column 0,1 of A
	columnShard1 := &pb.SparseMatrixShard{
		Row: []*pb.SparseMatrixShard_SparseRow{&pb.SparseMatrixShard_SparseRow{}, &pb.SparseMatrixShard_SparseRow{}},
	}
	columnShard1.Row[0].At = make(map[int32]float32)
	columnShard1.Row[1].At = make(map[int32]float32)

	columnShard1.Row[0].At[0] = 0.42
	columnShard1.Row[0].At[1] = 0.30
	columnShard1.Row[0].At[2] = 0.70

	columnShard1.Row[1].At[0] = 0.30
	columnShard1.Row[1].At[2] = 1.00

	bufColumn1, c1Err := proto.Marshal(columnShard1)

	// shard2: row 2 of A
	rowShard2 := &pb.SparseMatrixShard{
		Row: []*pb.SparseMatrixShard_SparseRow{&pb.SparseMatrixShard_SparseRow{}},
	}
	rowShard2.Row[0].At = make(map[int32]float32)

	rowShard2.Row[0].At[0] = 0.30
	rowShard2.Row[0].At[1] = 0.10
	rowShard2.Row[0].At[2] = 1.00

	bufRow2, r2Err := proto.Marshal(rowShard2)

	// shard2: column 2,3 of A
	columnShard2 := &pb.SparseMatrixShard{
		Row: []*pb.SparseMatrixShard_SparseRow{&pb.SparseMatrixShard_SparseRow{}, &pb.SparseMatrixShard_SparseRow{}},
	}
	columnShard2.Row[0].At = make(map[int32]float32)
	columnShard2.Row[1].At = make(map[int32]float32)

	columnShard2.Row[0].At[0] = 0.34
	columnShard2.Row[0].At[1] = 0.10
	columnShard2.Row[0].At[2] = 0.90

	columnShard2.Row[1].At[0] = 0.70
	columnShard2.Row[1].At[1] = 1.00

	bufColumn2, c2Err := proto.Marshal(columnShard2)

	if r1Err != nil || c1Err != nil || r2Err != nil || c2Err != nil {
		return nil, nil, nil, nil, fmt.Errorf("Failed generating mashalled buffer")
	}
	return bufRow1, bufColumn1, bufRow2, bufColumn2, nil
}
