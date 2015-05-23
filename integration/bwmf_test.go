package integration

import (
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/plutoshe/taskgraph"
	"github.com/plutoshe/taskgraph/example/bwmf"
	"github.com/plutoshe/taskgraph/example/topo"
	"github.com/taskgraph/taskgraph/controller"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/filesystem"
)

func TestBWMF(t *testing.T) {
	etcdURLs := []string{"http://localhost:4001"}

	job := "bwmf_basic_test"
	numOfTasks := uint64(2)

	generateTestData(t)

	ctl := controller.New(job, etcd.NewClient(etcdURLs), numOfTasks, []string{"Neighbors", "Master"})
	ctl.Start()

	tb := &bwmf.BWMFTaskBuilder{
		NumOfTasks: numOfTasks,
		ConfBytes: []byte(`{
			        "OptConf": {
						"Sigma":0.01,
						"Alpha":1,
						"Beta":0.1,
						"GradTol":1e-06,
						"FixedCnt": 200000,
					    "NumIters":4,
					    "DimLatent":2
					},
					"IOConf":  {
						"Fs":"local",
						"IDPath":"../.tmp/row_shard.dat",
						"ITPath":"../.tmp/column_shard.dat",
						"ODPath":"../.tmp/dShard.dat",
						"OTPath":"../.tmp/tShard.dat"
					}
				}`),
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(
			t,
			job,
			etcdURLs,
			tb,
			map[string]taskgraph.Topology{
				"Master":    topo.NewFullTopologyOfMaster(numOfTasks),
				"Neighbors": topo.NewFullTopologyOfNeighbor(numOfTasks),
			},
		)
	}

	ctl.WaitForJobDone()
	ctl.Stop()
}

func generateTestData(t *testing.T) {
	r0, c0, r1, c1 := getShards()
	fs := filesystem.NewLocalFSClient()
	wr0Err := bwmf.SaveMatrixShard(fs, r0, "../.tmp/row_shard.dat-000000")
	wr1Err := bwmf.SaveMatrixShard(fs, r1, "../.tmp/row_shard.dat-000001")
	wc0Err := bwmf.SaveMatrixShard(fs, c0, "../.tmp/column_shard.dat-000000")
	wc1Err := bwmf.SaveMatrixShard(fs, c1, "../.tmp/column_shard.dat-000001")
	if wr0Err != nil || wr1Err != nil || wc0Err != nil || wc1Err != nil {
		t.Errorf("Failed generating test data files: %s;%s;%s;%s.", wr0Err, wr1Err, wc0Err, wc1Err)
	}
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
func getShards() (row0, column0, row1, column1 *pb.MatrixShard) {
	// shard0: row 0,1 of A
	rowShard0 := &pb.MatrixShard{
		IsSparse: true,
		M:        2,
		N:        4,
		Val:      []float32{0.42, 0.30, 0.30, 0.34, 0.10, 0.70, 1.00},
		Ir:       []uint32{0, 1, 0, 0, 1, 0, 1},
		Jc:       []uint32{0, 2, 3, 5, 7},
	}

	// shard0: column 0,1 of A
	columnShard0 := &pb.MatrixShard{
		IsSparse: true,
		M:        2,
		N:        3,
		Val:      []float32{0.42, 0.30, 0.30, 0.70, 1.00},
		Ir:       []uint32{0, 1, 0, 0, 1},
		Jc:       []uint32{0, 2, 3, 5},
	}

	// shard1: row 1 of A
	rowShard1 := &pb.MatrixShard{
		IsSparse: true,
		M:        1,
		N:        4,
		Val:      []float32{0.70, 1.00, 0.90},
		Ir:       []uint32{0, 0, 0},
		Jc:       []uint32{0, 1, 2, 3, 3},
	}

	// shard1: column 2,3 of A
	columnShard1 := &pb.MatrixShard{
		IsSparse: true,
		M:        2,
		N:        3,
		Val:      []float32{0.34, 0.70, 0.10, 1.00, 0.90},
		Ir:       []uint32{0, 1, 0, 1, 0},
		Jc:       []uint32{0, 2, 4, 5},
	}

	return rowShard0, columnShard0, rowShard1, columnShard1
}
