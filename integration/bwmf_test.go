package integration

import (
	"fmt"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/golang/protobuf/proto"
	"github.com/taskgraph/taskgraph/controller"
	"github.com/taskgraph/taskgraph/example/bwmf"
	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
	"github.com/taskgraph/taskgraph/example/topo"
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
		NumIters:   4,
		ConfBytes: []byte(`{"OptConf": {"Sigma":0.01,"Alpha":1,"Beta":0.1,"GradTol":1e-06, "FixedCnt": 200000},
				    "IOConf":  {"Fs":"local",
						"IDPath":"../.tmp/row_shard.dat",
						"ITPath":"../.tmp/column_shard.dat",
						"ODPath":"../.tmp/dShard.dat",
						"OTPath":"../.tmp/tShard.dat"}}`),
		LatentDim: 2,
	}
	for i := uint64(0); i < numOfTasks; i++ {
		go drive(t, job, etcdURLs, tb, topo.NewFullTopology(numOfTasks))
	}

	ctl.WaitForJobDone()
	ctl.Stop()
}

func generateTestData(t *testing.T) {
	r1, c1, r2, c2, err := getBufs()
	if err != nil {
		t.Errorf("Failed preparing marshalled matrix shards: %s", err)
	}

	fsclient := filesystem.NewLocalFSClient()
	wr0, wr0Err := fsclient.OpenWriteCloser("../.tmp/row_shard.dat.0")
	wr1, wr1Err := fsclient.OpenWriteCloser("../.tmp/row_shard.dat.1")
	wc0, wc0Err := fsclient.OpenWriteCloser("../.tmp/column_shard.dat.0")
	wc1, wc1Err := fsclient.OpenWriteCloser("../.tmp/column_shard.dat.1")

	if wr0Err != nil || wr1Err != nil || wc0Err != nil || wc1Err != nil {
		t.Errorf("Failed generating test data files: %s;%s;%s;%s.", wr0Err, wr1Err, wc0Err, wc1Err)
	}
	wr0.Write(r1)
	wr1.Write(r2)
	wc0.Write(c1)
	wc1.Write(c2)
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
	rowShard1 := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{&pb.MatrixShard_RowData{}, &pb.MatrixShard_RowData{}},
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
	columnShard1 := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{&pb.MatrixShard_RowData{}, &pb.MatrixShard_RowData{}},
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
	rowShard2 := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{&pb.MatrixShard_RowData{}},
	}
	rowShard2.Row[0].At = make(map[int32]float32)

	rowShard2.Row[0].At[0] = 0.70
	rowShard2.Row[0].At[1] = 1.00
	rowShard2.Row[0].At[2] = 0.90

	bufRow2, r2Err := proto.Marshal(rowShard2)

	// shard2: column 2,3 of A
	columnShard2 := &pb.MatrixShard{
		Row: []*pb.MatrixShard_RowData{&pb.MatrixShard_RowData{}, &pb.MatrixShard_RowData{}},
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
