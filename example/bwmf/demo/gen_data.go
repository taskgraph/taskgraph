package main

import (
	"fmt"
	"github.com/golang/protobuf/proto"

	pb "github.com/taskgraph/taskgraph/example/bwmf/proto"
)

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
